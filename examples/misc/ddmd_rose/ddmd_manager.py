#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Async-friendly DDMD manager for orchestrating simulations
# ------------------------------------------------------------------------------

import asyncio
from collections import OrderedDict
from rose import Learner
from logger import Logger
import json


class DDMD_manager:
    """
    Orchestrates the scheduling, monitoring, and cancellation of simulations
    in an AI-steered ensemble simulation workflow.
    """

    def __init__(self, asyncflow):
        self.learner = Learner(asyncflow)
        self.logger = Logger(use_colors=True)
        self.registered_sims = OrderedDict()   # Active simulations: {tag: asyncio.Task}
        self.sim_task_queue = asyncio.Queue()  # Queue of pending simulation inputs
        self.time_between_predictions = 20     # If training is not performed, pause briefly before initiating the next prediction attempt.
 
    # --------------------------------------------------------------------------
    def check_prediction(self, pred):
        """
        Decide whether a simulation should be canceled based on prediction.
        Override this with actual logic.
        """
        raise NotImplementedError("check_prediction must be implemented")
    
    # --------------------------------------------------------------------------
    #
    def __del__(self):
        try:
            self.engine.shutdown()
        except:
            pass

    # --------------------------------------------------------------------------
    #
    def close(self):
        try:
            self.engine.shutdown()
            print('Engine is shutdown')
        except:
            pass

    # --------------------------------------------------------------------------
    #
    def stop(self):
        try:
            self.engine.shutdown()
            print('Engine is shutdown')
        except:
            pass

    # --------------------------------------------------------------------------
    async def submit_sims(self):
        """
        Submit simulations from the queue and register them.
        """
        while True:
            await self.monitor_sims()  # Clean up completed/failed tasks

            if self.next_batch_size <= 0:
                await asyncio.sleep(1)
                continue

            for _ in range(self.next_batch_size):
                try:
                    sim_inputs = self.sim_task_queue.get_nowait()
                except asyncio.QueueEmpty:
                    self.logger.info("No more simulation inputs in queue.")
                    return

                simul = self.simulation(sim_inputs=sim_inputs)
                sim_tag = sim_inputs['sim_tag']
                self.logger.task_started(f'Simulation {sim_tag}')
                self.registered_sims[sim_tag] = simul 

                if self.sim_task_queue.empty():
                    self.logger.info("Last simulation has been submitted.")
                    break

            if self.next_batch_size > 0:
                self.logger.info(
                    f"[DDMD] Submitted {self.next_batch_size} new simulation(s)"
                )
                self.next_batch_size = 0  # Reset after submission

            await asyncio.sleep(0.5)

    # --------------------------------------------------------------------------
    async def monitor_sims(self):
        """
        Unregister completed or failed simulations and prepare to submit next batch of simulations.
        """
        unregister_sims = []

        for sim_tag, task in list(self.registered_sims.items()):
            if task.done():
                unregister_sims.append(sim_tag)
                self.logger.task_completed(f"Simulation {sim_tag}")
                self.next_batch_size += 1

                if task.exception():
                    self.logger.error(
                        f"Simulation {sim_tag} failed: {task.exception()}"
                    )

        for tag in unregister_sims:
            self.registered_sims.pop(tag, None)

        if unregister_sims and not self.sim_task_queue.empty():
            self.next_batch_size = min(self.next_batch_size, self.max_sim_batch)
            self.logger.info(
                f"{self.next_batch_size} simulations will start at next iteration"
            )

    # --------------------------------------------------------------------------
    async def cancel_sims(self):
        """
        Cancel simulations that meet the prediction-based stop criteria.
        """
        unregister_sims = []

        for sim_tag, pred in self.sim_predictions.items():
            self.logger.info(f"{sim_tag} prediction: {pred}")

            if sim_tag not in self.registered_sims:
                continue

            if self.check_prediction(prediction=pred):
                task = self.registered_sims[sim_tag]
                task.cancel()
                unregister_sims.append(sim_tag)
                self.logger.task_killed(
                    f"Simulation {sim_tag} canceled due to prediction score {pred}"
                )
                self.next_batch_size += 1

        for tag in unregister_sims:
            self.registered_sims.pop(tag, None)

        if unregister_sims and not self.sim_task_queue.empty():
            self.next_batch_size = min(self.next_batch_size, self.max_sim_batch)
            self.logger.info(
                f"{self.next_batch_size} simulations will start at next iteration"
            )

    # --------------------------------------------------------------------------
    async def teach(self):
        """
        Main event loop: orchestrates input collection, submissions,
        predictions, cancellations, and monitoring.
        """
        self.logger.separator("DDMD MANAGER STARTING")
        await self.collect_sim_inputs()

        submit_task = asyncio.create_task(self.submit_sims())

        while True:
            self.logger.info(f"{len(self.registered_sims)} simulation(s) running...")

            if self.retrain_model:
                await self.train_model()
            else:
                await asyncio.sleep(self.time_between_predictions) 

            self.logger.task_started("Prediction")
            sim_inds = list(self.registered_sims.keys())
            predictions = await self.prediction(sim_inds=sim_inds, sim_output_dir=self.sim_output_dir)

            # # ************************
            # # Use the following call for calling predicions as executable
            # await self.exe_prediction(sim_inds=sim_inds, sim_output_dir=self.sim_output_dir)
            # with open(self.prediction_file, 'r') as f:
            #     predictions = json.load(f) 
            # # ************************

            self.sim_predictions = predictions
            self.logger.info(f'[Task-Prediction] completed with {len(predictions)} new results.')
            if predictions:
                await self.cancel_sims()

            if self.sim_task_queue.empty() and not self.registered_sims:
                break

            await asyncio.sleep(1) 

        await submit_task
        self.logger.manager_exiting()
        self.logger.separator("DDMD MANAGER FINISHED")