#!/usr/bin/env python3

# ------------------------------------------------------------------------------
#
import json
import asyncio
#from rose import TaskConfig
#from rose.uq.uq_learner import ParallelUQLearner
from rose import Learner
#from rose.uq.uq_learner import ParallelUQLearner
from ddmd_newLearner import DDMDLearner
import time
from logger import Logger
from collections import OrderedDict
from typing import Callable, Dict, Any, Optional, List, Union, Tuple, Iterator



class DDMD_manager(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, asyncflow):

        self.learner = Learner(asyncflow)
        #self.learner = DDMDLearner(asyncflow)
        self.registered_sims = OrderedDict()
        self.submit_next_sim = True
        self.logger: Logger = Logger(use_colors=True)
        self.last_sim_ind = 0

        self._debug = False

        if len(self.sim_tags) == 0:
            raise RuntimeError("# Simulation IDs must be initialized before this point")

    # --------------------------------------------------------------------------
    #
    # def set_sim_ids(self):
    #     raise NotImplementedError
    
    # def get_sim_tag(self, *args, **kwargs):
    #     # Returns the name of the simulation at the specified index
    #     # Args:    
    #     #       index
    #     # Returen: 
    #     #       Simulation name
    #     raise NotImplementedError
    
    # --------------------------------------------------------------------------
    #
    def store_registered_sims(self):
        # Store names of all registered simulations
        raise NotImplementedError

    # # --------------------------------------------------------------------------
    # #
    # def start_training(self, *args, **kwargs):
    #     # Determine if the simulation has generated sufficient data for model training.
    #     #
    #     # Args:
    #     #     active_learning_output: Output from the active learning process.
    #     #
    #     # Returns:
    #     #     True if model training can begin; False otherwise.

    #     raise NotImplementedError
        
    # --------------------------------------------------------------------------
    #    
    def check_prediction(self, *args, **kwargs):
        # Determine whether the simulation should be canceled based on the model's prior prediction.
        # 
        # Args:
        #     prediction: Model prediction for the current simulation.
        #
        # Returns:
        #     True if the simulation should be canceled; False otherwise.

        raise NotImplementedError
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
        except:
            pass


    # --------------------------------------------------------------------------
    #
    def wait(self, t=1):
        time.sleep(t)

    # --------------------------------------------------------------------------
    #
    def stop(self):
        try:
            self.engine.shutdown()
        except:
            pass
    
    # --------------------------------------------------------------------------
    #
    def submit_sim_batch(self):
        # Submit the next batch of simulations and register each one


        if not self.submit_next_sim or self.sim_batch_size == 0:
            return
        print(f'[DDMD] submitting {self.sim_batch_size} simulation(s)')
        for i in range(self.sim_batch_size):

            self.last_sim_ind += 1
            sim_kwargs = next(self.next_input)
            sim_config = self.learner.simulation_function.copy()
            sim_config['kwargs'] = sim_kwargs
            simul = self.learner._register_task(sim_config)
            sim_tag = sim_kwargs['sim_tag']
            self.logger.task_started(f'Simulation {sim_tag} registered')
            self.registered_sims[sim_tag] = simul 
            if self.last_sim_ind == self.total_sim:
                self.submit_next_sim = False
                self.logger.info(f'Last simulation has been submitted with {self.last_sim_ind} total ...')
                break
        self.store_registered_sims()
        self.sim_batch_size = 0
        
    # --------------------------------------------------------------------------
    #        
    def unregister_sims(self, unregistered_sims=[]):
        # Unregister all tasks that have finished execution or were canceled
        #unregistered_sims = []
        self.sim_batch_size = len(unregistered_sims)
        for sim_tag, task in self.registered_sims.items():
            if sim_tag not in unregistered_sims:
                if task.done():
                    unregistered_sims.append(sim_tag)
                    self.sim_batch_size += 1
                    self.logger.task_completed(f'{sim_tag} state is done')
                # elif task.cancelled():
                #     unregistered_sims.append(sim_tag)
                #     self.sim_batch_size += 1
                #     self.logger.task_completed(f'{sim_tag} state is cancelled')
                else:
                    #self.logger.task_log(f'{sim_tag} state is running')
                    pass


        if len(unregistered_sims) > 0 :
            # Remove all tasks that are no longer registered
            self.registered_sims = {key:val for key, val in  self.registered_sims.items() if key not in  unregistered_sims}
            self.store_registered_sims()

        if self.submit_next_sim:
            # Set the simulation batch size for the next submission
            self.sim_batch_size = min(self.sim_batch_size, self.max_sim_batch_size)
            self.logger.info(f'{self.sim_batch_size} simulations will start at next iteration')
    # --------------------------------------------------------------------------
    #        
    def calcel_sim(self):

        if self._debug:
            unresolved = self.learner.unresolved
            print('unresolved:', unresolved)
            print('keys:', self.registered_sims.keys())
        # with open(self.prediction_filename, 'r') as f:
        #     loaded_data = json.load(f)

        unregistered_sims = []
        for sim_tag, pred in self.predictions.items():
            self.logger.info(f'{sim_tag} simulations got prediction {pred}')
            if sim_tag in self.registered_sims.keys():
                if self._debug:
                    print('task:', self.registered_sims[sim_tag])
                cancel_task = self.check_prediction(pred=pred)
                if cancel_task:
                    task = self.registered_sims[sim_tag]
                    unregistered_sims.append(sim_tag)
                    try:
                        task.cancel()
                        self.logger.task_killed(f'Cancelling {sim_tag} with prediction score {pred}: ROSE task ID {task.id}')
                    except Exception as e:
                        self.logger.error(f'An error occurred: {e}. Unable to cancel simulation {sim_tag}.')
                else:
                    self.logger.info(f'prediction for {sim_tag} simulations is {pred} => no calcelation yet')
        
        # Tasks canceled by ROSE must be unregistered.
        self.unregister_sims(unregistered_sims=unregistered_sims)

    # --------------------------------------------------------------------------
    #
    async def teach(self):

        self.logger.separator("DDMD MANAGER STARTING")

        run_simulations = True


        while run_simulations:

            # Submit the initial batch of simulations and pause before initiating active learning training.
            self.submit_sim_batch()

            if self.retrain_model:

                for acl_iter in range(self.training_epochs):

                    self.logger.info(f'\nStarting Training Iteration-{acl_iter}')
                    self.logger.info(f'{len(self.registered_sims)} simulation(s) running....')
                    self.logger.task_started('Training')
                    train = await self.training()
                    
                    self.logger.task_started('Check Accuracy')
                    (should_stop, metric_val) = await self.check_accuracy(train)
                    if should_stop:
                        self.logger.info(f'Accuracy ({metric_val}) met the threshold, breaking...')
                        break

                    self.logger.task_started(f'Active Learning iteration {acl_iter}')
                    await self.active_learn()

                    self.unregister_sims()
                    self.submit_sim_batch()
                    if self._debug:
                        time.sleep(20)

            if self._debug:
                time.sleep(10)
            # 

            self.logger.info(f'{len(self.registered_sims)} simulation(s) running....')
            self.logger.task_started('Prediction')
            await self.prediction()
            
            self.calcel_sim()

            # Continue running until there are no more simulations to submit and no more registered simulations.
            if not self.submit_next_sim and len(self.registered_sims) == 0:
                run_simulations = False


        self.logger.manager_exiting()
        self.logger.separator("DDMD MANAGER FINISHED")
