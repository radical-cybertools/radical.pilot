import asyncio
import os
import sys
import random
import shutil
import numpy as np
from pathlib import Path
from ddmd_manager import DDMD_manager
from rose.metrics import MODEL_ACCURACY

VAL_SPLIT = 0.2
MIN_TRAIN_SIZE = 1

class DummyWorkflow(DDMD_manager):
    """Dummy workflow for managing DDMD simulations, training, and predictions."""

    def __init__(self, **kwargs):
        home_dir = Path(kwargs.get('home_dir', Path.home() / 'DDMD'))

        self.sim_output_dir = self._ensure_dir(kwargs.get('sim_output_dir', home_dir / 'sim_output'))
        self.sim_inputs_dir  = self._ensure_dir(kwargs.get('sim_inputs_dir',  home_dir / 'sim_input'))
        self.train_dir      = self._ensure_dir(kwargs.get('train_dir',      home_dir / 'train'))
        self.train_al_dir   = self._ensure_dir(kwargs.get('train_al_dir',      home_dir / 'train_al'))
        self.val_dir        = self._ensure_dir(kwargs.get('val_dir',        home_dir / 'val'))

        self.max_sim_batch       = kwargs.get('max_sim_batch', 4)
        self.next_batch_size     = self.max_sim_batch
        self.training_threshold  = kwargs.get('training_threshold', 0.95)
        self.prediction_threshold = kwargs.get('prediction_threshold', 0.5)
        self.training_epochs     = kwargs.get('training_epochs', 1)

        self.retrain_model = self.training_epochs > 0
        self.sim_predictions = {}

        self.code_path = f'{sys.executable} {os.getcwd()}'
        self.model_filename = home_dir / 'model.pkl'
        self.prediction_file  = home_dir / 'predicions.json'

        asyncflow = kwargs.get('asyncflow')
        super().__init__(asyncflow)

        self._register_learner_tasks()

        self.generate_sim_inputs(self.sim_inputs_dir, num_files=10)

    # --------------------------------------------------------------------------
    #    
    @staticmethod
    def _ensure_dir(path):
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    # --------------------------------------------------------------------------
    #    
    @staticmethod
    def generate_sim_inputs(sim_inputs_dir, num_files: int = 5):
        """
        Ensure all files from previous run are deleted and new dummy input files are generated
 
        Args:
            sim_inputs_dir: Path to the simulation input directory.
            num_files: Number of dummy files to create.
        """
        sim_inputs_path = Path(sim_inputs_dir)
        if sim_inputs_path.exists() and sim_inputs_path.is_dir():
            shutil.rmtree(sim_inputs_path)
        sim_inputs_path.mkdir(parents=True, exist_ok=True)

        # Generate dummy input files
        for i in range(num_files):
            file_path = sim_inputs_path / f"input_{i}.npz"
            X = np.random.rand(100, 1)  # Example input array
            np.savez(file_path, X=X)
            #print(f"Generated input file: {file_path}")
    # --------------------------------------------------------------------------
    #  
    def check_prediction(self, *args, **kwargs):
        return kwargs['prediction'] < self.prediction_threshold
    
    # --------------------------------------------------------------------------
    #    
    async def collect_sim_inputs(self):
        """Collect simulation inputs"""
        filenames = await asyncio.to_thread(lambda: list(self.sim_inputs_dir.iterdir()))
        for filename in filenames:
            if filename.is_file():
                sim_tag = f'sim_{filename.name}'
                await self.sim_task_queue.put({'sim_input': filename, 'sim_tag': sim_tag})

    # --------------------------------------------------------------------------
    #    
    def _register_learner_tasks(self):

        @self.learner.simulation_task
        async def simulation(*args, **kwargs):
            sim_tag = kwargs["sim_inputs"]["sim_tag"]
            sim_input = kwargs["sim_inputs"]["sim_input"]
            args = f'--input_dir {sim_input} --output_dir {self.sim_output_dir} --sim_tag {sim_tag}'
            return f'{self.code_path}/simulation.py {args}'
        self.simulation = simulation

    # --------------------------------------------------------------------------
        @self.learner.training_task
        async def training(*args, **kwargs):
            args = (
                f'--model_filename {self.model_filename} '
                f'--sim_output_dir {self.sim_output_dir} '
                f'--train_dir {self.train_al_dir} --val_dir {self.val_dir}'
            )
            return f'{self.code_path}/train.py {args}'
        self.training = training

    # --------------------------------------------------------------------------
        @self.learner.active_learn_task
        async def active_learn(*args, **kwargs):
            args = (
                f'--model_filename {self.model_filename} '
                f'--train_dir {self.train_dir} '
                f'--train_al_dir {self.train_al_dir}'
            )
                        
            return f'{self.code_path}/active_learn.py {args}'
        self.active_learn = active_learn   

    # --------------------------------------------------------------------------
        @self.learner.utility_task
        async def exe_prediction(*args, **kwargs):
            args = (
                f'--model_filename {self.model_filename} '
                f'--sim_output_dir {self.sim_output_dir} '
                f'--output_file {self.prediction_file}'
            )
            return f'{self.code_path}/predict.py {args}'
        self.exe_prediction = exe_prediction  

    # --------------------------------------------------------------------------
        @self.learner.utility_task(as_executable=False)
        async def prediction(*args, **kwargs):
            """Generate random sim_predictions."""
            sim_inds = kwargs["sim_inds"]
            sim_output_dir = kwargs["sim_output_dir"]
            sim_predictions = {}
            for sim_ind in sim_inds:
                sim_dir = Path(sim_output_dir, sim_ind)
                if sim_dir.is_dir():
                    sim_predictions[sim_dir.name] = random.random()
            return sim_predictions
        self.prediction = prediction

        # @self.learner.utility_task(as_executable=False)
        # async def prediction(*args, **kwargs):
        #     masha = 1
        #     return
        # self.prediction = prediction

    # --------------------------------------------------------------------------
        @self.learner.as_stop_criterion(metric_name=MODEL_ACCURACY, threshold=self.training_threshold)
        async def check_accuracy(*args, **kwargs):
            args = f'--model_filename {self.model_filename} --val_dir {self.val_dir}'
            return f'{self.code_path}/check_accuracy.py {args}'
        self.check_accuracy = check_accuracy

    # --------------------------------------------------------------------------
    #    
    async def train_model(self):
        """Run training loop until accuracy threshold is met or epochs are exhausted."""
        for epoch in range(self.training_epochs):
            self.logger.info(f'\nStarting Training Epoch {epoch}')
            self.logger.info(f'{len(self.registered_sims)} simulation(s) running....')
            
            train_task = self.training()
            self.logger.task_started('Training Model')
            
            should_stop, metric_val = await self.check_accuracy(train_task)
            self.logger.task_started('Check Accuracy')

            if should_stop:
                self.logger.info(f'Accuracy ({metric_val}) met the threshold, stopping training...')
                self.retrain_model = False
                break

            await self.active_learn()
            self.logger.task_started('Active Learning')
