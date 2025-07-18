import os
import sys
import json
from ddmd_manager import DDMD_manager
from rose.engine import Task
from rose.metrics import MODEL_ACCURACY
#import radical.utils as ru
from pathlib import Path

# --------------------------------------------------------------------------
#
class dummy_workflow(DDMD_manager):
    def __init__(self, **kwargs):

        home_dir = Path.home() /'DDMD'
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

        self.max_sim_batch_size = kwargs.get('max_sim_batch_size', 1)
        self.sim_batch_size = self.max_sim_batch_size
        self.init_sim_time = kwargs.get('init_sim_time', 10)

        self.training_threshold = kwargs.get('training_threshold', 0.95)
        self.prediciont_threshold = kwargs.get('prediciont_threshold', 1.5)
        self.training_epochs = kwargs.get('training_epochs', 1)
        
        self.retrain_model = True   #retrain model after first convergens 

        self.sim_inputs = []
        self.sim_tags = []
        self.total_sim = 0

        self.code_path = f'{sys.executable} {os.getcwd()}'
        self.model_filename =  home_dir / 'model.pkl'
        self.registered_sims_filename = home_dir / 'registered_sims.json'
        self.prediction_filename =  home_dir / 'prediction.json'
        self._collect_sim_inputs()
        self.next_input = self._get_next_input()

        self.resources = kwargs.get('resources', {'runtime': 30, 'resource': 'local.localhost'})
        super().__init__(self.resources)

        self._register_learner_tasks()
        
    # --------------------------------------------------------------------------
    #
    def start_training(self, *args, **kwargs):
        return 'start' in kwargs['al_result']

    # --------------------------------------------------------------------------
    #
    def get_sim_tag(self, *args, **kwargs):
        try:
            return self.sim_tags[kwargs['ind']]
        except:
            raise RuntimeError(f"Simulation tag not found for the specified index: {kwargs.ind}")
    # --------------------------------------------------------------------------
    #

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
                # output_path = self.sim_output_path / sim_tag
                # output_path.mkdir(parents=True, exist_ok=True)

                # #Create directory for simulation outputs
                self.sim_tags.append(sim_tag)
                #self.set_sim_ids(sim_tag)

    # --------------------------------------------------------------------------
    #
    def _get_next_input(self):
        #Generator to get next input file for simulation
        for i in range(self.total_sim):
            if i == self.total_sim -1:
                self.submit_next_sim = False
            yield (self.sim_inputs[i],self.sim_tags[i])

    # --------------------------------------------------------------------------
    #
    def _register_learner_tasks(self):

        # Define and register the simulation task
        @self.learner.simulation_task
        def simulation(*args, **kwargs):
            sim_input, sim_tag = next(self.next_input)
            sim_args = f' --input_path {sim_input} --output_path {self.sim_output_path} --sim_tag {sim_tag} '
            print(sim_args)
            return Task(executable=f'{self.code_path}/simulation.py {sim_args}')
        self.simulation = simulation

        # Define and register the training task
        @self.learner.training_task
        def training(*args, **kwargs):
            train_args = f'--model_filename {self.model_filename} --train_path {self.train_path}'
            return Task(executable=f'{self.code_path}/train.py {train_args}')
        self.training = training

        # Define and register the active learning task
        @self.learner.active_learn_task
        def active_learn(*args, **kwargs):
            al_args = f'--sim_output_path {self.sim_output_path} --registered_sims_filename {self.registered_sims_filename}  --train_path {self.train_path} --val_path {self.val_path} '
            return Task(executable=f'{self.code_path}/active_learn.py {al_args}')
        self.active_learn = active_learn

        # Defining the stop criterion with a metric (MSE in this case)
        @self.learner.as_stop_criterion(metric_name=MODEL_ACCURACY, threshold=self.training_threshold)
        def check_accuracy(*args, **kwargs):
            val_args = f'--model_filename {self.model_filename} --val_dir {self.val_path}'
            return Task(executable=f'{self.code_path}/check_accuracy.py {val_args}')
        self.check_accuracy = check_accuracy

        # Define and register the prediction task
        @self.learner.utility_task
        def prediction(*args, **kwargs):
            prediction_args = f'--model_filename {self.model_filename} --sim_output_path {self.sim_output_path} --registered_sims_filename {self.registered_sims_filename} --output_file {self.prediction_filename}'
            return Task(executable=f'{self.code_path}/predict.py {prediction_args}')
        self.prediction = prediction

