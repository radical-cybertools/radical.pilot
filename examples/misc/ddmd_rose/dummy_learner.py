import os
import sys
import json
from ddmd_manager import DDMD_manager
from rose.metrics import MODEL_ACCURACY
from pathlib import Path
import random
#import pickle
from pathlib import Path
import numpy as np
import time
import random


VAL_SPLIT = 0.2
MIN_TRAIN_SIZE = 1

# --------------------------------------------------------------------------
#
class dummy_workflow(DDMD_manager):
    def __init__(self, **kwargs):

        home_dir = kwargs.get('home_dir', Path.home() /'DDMD')   
        home_dir = Path(home_dir)
        self.sim_output_path = kwargs.get('sim_output_path', home_dir / 'sim_output')    
        self.sim_output_path = Path(self.sim_output_path)
        if not self.sim_output_path.is_dir():
            self.sim_output_path.mkdir(parents=True, exist_ok=True) 

        self.sim_input_path = kwargs.get('sim_input_path',  home_dir / 'sim_input')
        self.sim_input_path = Path(self.sim_input_path)
        if not self.sim_input_path.is_dir():
            self.sim_input_path.mkdir(parents=True, exist_ok=True) 

        self.train_path = kwargs.get('train_path',  home_dir / 'train')
        self.train_path = Path(self.train_path)
        if not self.train_path.is_dir():
            self.train_path.mkdir(parents=True, exist_ok=True) 

        self.val_path = kwargs.get('val_path',  home_dir / 'val')
        self.val_path = Path(self.val_path)
        if not self.val_path.is_dir():
            self.val_path.mkdir(parents=True, exist_ok=True) 

        self.max_sim_batch_size = kwargs.get('max_sim_batch_size', 4)
        self.sim_batch_size = self.max_sim_batch_size
        self.init_sim_time = kwargs.get('init_sim_time', 10)

        self.training_threshold = kwargs.get('training_threshold', 0.95)
        self.prediciont_threshold = kwargs.get('prediciont_threshold', 1.5)
        self.training_epochs = kwargs.get('training_epochs', 1)
        
        self.retrain_model = True   #retrain model after first convergens 

        self.sim_inputs = []
        self.sim_tags = []
        self.predictions = {}
        self.total_sim = 0

        self.code_path = f'{sys.executable} {os.getcwd()}'
        self.model_filename =  home_dir / 'model.pkl'
        self.registered_sims_filename = home_dir / 'registered_sims.json'
        self.prediction_filename =  home_dir / 'prediction.json'
        self._collect_sim_inputs()
        self.next_input = self._get_next_input()

        asyncflow = kwargs.get('asyncflow')
        super().__init__(asyncflow)

        self._register_learner_tasks()

    def check_prediction(self, *args, **kwargs):
        return kwargs['pred'] < self.prediciont_threshold

    # --------------------------------------------------------------------------
    #
    def store_registered_sims(self, *args, **kwargs):
        with open(self.registered_sims_filename, 'w') as f:
            json.dump(list(self.registered_sims.keys()), f)

    # --------------------------------------------------------------------------
    #
    def _collect_sim_inputs(self):

        for filename in self.sim_input_path.iterdir():
            input_path = self.sim_input_path / filename
            if input_path.is_file:
                # This file will be used as input for simulation 
                self.sim_inputs.append(input_path)
                self.total_sim += 1
                sim_tag = f'sim_{filename.name}'
                # #Create directory for simulation outputs
                self.sim_tags.append(sim_tag)

    # --------------------------------------------------------------------------
    #
    def _get_next_input(self):
        #Generator to get next input file for simulation
        for i in range(self.total_sim):
            if i == self.total_sim -1:
                self.submit_next_sim = False
            yield ({'sim_input': self.sim_inputs[i], 'sim_tag': self.sim_tags[i]})
            #yield (self.sim_inputs[i], self.sim_tags[i])

    # --------------------------------------------------------------------------
    #
    def _register_learner_tasks(self):

        # # # Define and register the simulation task
        # @self.learner.simulation_task(as_executable=False) 
        # async def simulation(sim_ind=0):
        #     input_path='input.file'
        #     output_path='output.dir'

        #     sim_tag = f'sim_{sim_ind}'
            
        #     print('input_path', input_path)
        #     print(f'Simulation will read from {input_path}')
        #     output_sim_path = Path(output_path, sim_tag)
        #     if not output_sim_path.is_dir():
        #         output_sim_path.mkdir(parents=True, exist_ok=True)
            
        #     for i in range(150):
                
        #         X = np.random.uniform(low=0.0, high=1.0, size=500)
        #         #y = complicated_function(X)
        #         X = X.reshape(-1, 1)
        #         output_file = f'{output_sim_path}/r{sim_tag}_{i}.npz'
        #         np.savez(output_file, X=X, y=y)
        #         print(f'Saved simulation {i} to {output_file}')
        #     t = random.randint(10,15)
        #     time.sleep(t)

        #     #print(f'Simulation completed and all results saved to {output_file}')
        #     return
        # self.simulation = simulation

        @self.learner.simulation_task
        async def simulation(*args, **kwargs):
            sim_tag = kwargs.get("sim_tag")
            sim_input = kwargs.get("sim_input")
            sim_args = f' --input_path {sim_input} --output_path {self.sim_output_path} --sim_tag {sim_tag} '
            return f'{self.code_path}/simulation.py {sim_args}'
        self.simulation = simulation


        # Define and register the training task
        @self.learner.training_task
        async def training(*args, **kwargs):
            train_args = f'--model_filename {self.model_filename} ' \
                         f'--sim_output_path {self.sim_output_path} '\
                        f'--registered_sims_filename {self.registered_sims_filename} '\
                        f'--train_path {self.train_path} --val_path {self.val_path} '
            return f'{self.code_path}/train.py {train_args}'
        self.training = training

        # Define and register the active learning task
        @self.learner.active_learn_task
        async def active_learn(*args, **kwargs):
            return f'{self.code_path}/active_learn.py '
        self.active_learn = active_learn
        
        # # Define and register the prediction task
        # @self.learner.utility_task
        # async def prediction(*args, **kwargs):
        #     prediction_args = f'--model_filename {self.model_filename} --sim_output_path {self.sim_output_path} --registered_sims_filename {self.registered_sims_filename} --output_file {self.prediction_filename}'
        #     return f'{self.code_path}/predict.py {prediction_args}'
        # self.prediction = prediction         

        # Define and register the prediction task
        @self.learner.utility_task(as_executable=False) 
        async def prediction():
            self.predictions = {}
            for sim_ind in self.registered_sims.keys():
                sim_dir = Path(self.sim_output_path, sim_ind)
                if sim_dir.is_dir():
                    self.predictions[sim_dir.name] = random.random()
            print(f'\n\n[Taks-Prediction] completed with total {len(self.predictions)} new predictions .')
            return
        self.prediction = prediction  


        # Defining the stop criterion with a metric (MSE in this case)
        @self.learner.as_stop_criterion(metric_name=MODEL_ACCURACY, threshold=self.training_threshold)
        async def check_accuracy(*args, **kwargs):
            val_args = f'--model_filename {self.model_filename} --val_dir {self.val_path}'
            return f'{self.code_path}/check_accuracy.py {val_args}'
        self.check_accuracy = check_accuracy