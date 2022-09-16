#!/usr/bin/env python3

"""Perform GPR Active Learning where simulations are sent in batches"""
from pathlib import Path

from colmena.models import ExecutableTask
from colmena.thinker import BaseThinker, agent
from colmena.task_server import ParslTaskServer
from colmena.redis.queue import ClientQueues, make_queue_pairs
from sklearn.gaussian_process import GaussianProcessRegressor, kernels
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import Pipeline
from radical.pilot.agent.executing.redis_parsl_rp import RedisRadicalExecutor
from parsl.providers import LocalProvider

from functools import partial, update_wrapper
from parsl.config import Config
from datetime import datetime
import numpy as np
import argparse
import logging
import json
import sys
import os

from sim import Simulation

class Thinker(BaseThinker):
    """Tool that monitors results of simulations and calls for new ones, as appropriate"""

    def __init__(self, queues: ClientQueues, output_dir: str, n_guesses: int = 100, batch_size: int = 10):
        """
        Args:
            output_dir (str): Output path
            batch_size (int): Number of simulations to run in parallel
            n_guesses (int): Number of guesses the Thinker can make
            queues (ClientQueues): Queues for communicating with task server
        """
        super().__init__(queues)
        self.n_guesses = n_guesses
        self.queues = queues
        self.batch_size = batch_size
        self.dim = 1
        self.output_path = os.path.join(output_dir, 'results.json')

    @agent
    def optimize(self):
        """Connects to the Redis queue with the results and pulls them"""

        # Make a random guess to start
        for i in range(self.batch_size):
            self.queues.send_inputs(np.random.uniform(-32.768, 32.768, size=(self.dim,)).tolist())
        self.logger.info('Submitted initial random guesses to queue')
        train_X = []
        train_y = []

        # Use the initial guess to train a GPR
        gpr = Pipeline([
            ('scale', MinMaxScaler(feature_range=(-1, 1))),
            ('gpr', GaussianProcessRegressor(normalize_y=True, kernel=kernels.RBF() * kernels.ConstantKernel()))
        ])

        # Make guesses based on expected improvement
        while not self.done.is_set():
            # Wait for the results to complete
            with open(self.output_path, 'a') as fp:
                for _ in range(self.batch_size):
                    result = self.queues.get_result()
                    print(result.json(), file=fp)

                    if not result.success:
                        raise ValueError(f'Task failed: {result.failure_info.exception}. See {self.output_path} for full details')

                    # Store the result
                    train_X.append(result.args)
                    train_y.append(result.value)

            if len(train_X) > self.n_guesses:
                break

            # Update the GPR with the available training data
            gpr.fit(np.vstack(train_X), train_y)

            # Generate a random assortment of potential next points to sample
            sample_X = np.random.uniform(size=(self.batch_size * 1024, self.dim), low=-32.768, high=32.768)

            # Compute the expected improvement for each point
            pred_y, pred_std = gpr.predict(sample_X, return_std=True)
            best_so_far = np.min(train_y)
            ei = (best_so_far - pred_y) / pred_std

            # Run the samples with the highest EI
            best_inds = np.argsort(ei)[-self.batch_size:]
            self.logger.info(f'Selected {len(best_inds)} best samples. EI: {ei[best_inds]}')
            for i in best_inds:
                best_ei = sample_X[i, :]
                self.queues.send_inputs(best_ei.tolist())
            self.logger.info('Sent all of the inputs')

        self.logger.info('Done!')


if __name__ == '__main__':
    # User inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-host", default="127.0.0.1",
                        help="Address at which the redis server can be reached")
    parser.add_argument("--redis-port", default="6379",
                        help="Port on which redis is available")
    parser.add_argument("--num-guesses", "-n", help="Total number of guesses", type=int, default=100)
    parser.add_argument("--num-parallel", "-p", help="Number of guesses to evaluate in parallel (i.e., the batch size)",
                        type=int, default=os.cpu_count())
    args = parser.parse_args()
    password = None
    # Connect to the redis server
    client_queues, server_queues = make_queue_pairs(args.redis_host, args.redis_port, password, serialization_method='json')

    # Make the output directory
    out_dir = os.path.join('runs',
                           f'batch-N{args.num_guesses}-P{args.num_parallel}'
                           f'-{datetime.now().strftime("%d%m%y-%H%M%S")}')
    os.makedirs(out_dir, exist_ok=False)
    with open(os.path.join(out_dir, 'params.json'), 'w') as fp:
        run_params = args.__dict__
        run_params['file'] = os.path.basename(__file__)
        json.dump(run_params, fp)

    # Set up the logging
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO,
                        handlers=[logging.FileHandler(os.path.join(out_dir, 'runtime.log')),
                                  logging.StreamHandler(sys.stdout)])

    # Write the configuration
    config = Config(executors = [RedisRadicalExecutor(label = 'RedisRadicalExecutor',
                                 resource       = 'local.localhost',
                                 login_method   = '',
                                 project        = '',
                                 partition      = '',
                                 walltime       = 240,
                                 managed        = True,
                                 max_tasks      = 12,
                                 enable_redis   = True,
                                 redis_port     = int(args.redis_port),
                                 redis_host     = args.redis_host,
                                 redis_pass     = password)])
    config.run_dir = os.path.join(out_dir, 'run-info')

    # Create the task server and task generator
    my_sim = Simulation(Path('./simulate'))
    doer = ParslTaskServer([my_sim], server_queues, config)
    thinker = Thinker(client_queues, out_dir, n_guesses=args.num_guesses, batch_size=args.num_parallel)
    logging.info('Created the task server and task generator')

    try:
        # Launch the servers
        doer.start()
        thinker.start()
        logging.info('Launched the servers')

        # Wait for the task generator to complete
        thinker.join()
        logging.info('Task generator has completed')
    finally:
        client_queues.send_kill_signal()

    # Wait for the task server to complete
    doer.join()
