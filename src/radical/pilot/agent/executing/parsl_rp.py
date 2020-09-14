"""RADICAL-Executor builds on the RADICAL-Pilot/ParSL
"""

from concurrent.futures import Future

import radical.pilot as rp
import radical.utils as ru

import typeguard
import logging
import threading
import queue
import pickle
import os
from multiprocessing import Process, Queue
from typing import Any, Dict, List, Optional, Tuple, Union

from ipyparallel.serialize import unpack_apply_message  # ,unpack_apply_message
from ipyparallel.serialize import deserialize_object  # ,serialize_object

from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.errors import *
from parsl.executors.base import ParslExecutor

from parsl.utils import RepresentationMixin
from parsl.providers import LocalProvider


logger = logging.getLogger(__name__)
report = ru.Reporter(name='radical.pilot')

class RADICALExecutor(ParslExecutor, RepresentationMixin):
    """Executor designed for cluster-scale

    The RADICALExecutor system has the following components:

      1. "start" resposnible for creating the RADICAL-executor session and pilot.
      2. "submit" resposnible for translating and submiting ParSL tasks the RADICAL-executor.
      3. "shut_down"  resposnible for shutting down the RADICAL-executor components.

    Here is a diagram

    .. code:: python
                                                                                                                     RADICAL Executor
        ----------------------------------------------------------------------------------------------------------------------------------------------------
                        ParSL API                          |         ParSL DFK/dflow               |      Task Translator      |     RP-Client/Unit-Manager
        ---------------------------------------------------|---------------------------------------|---------------------------|----------------------------                                                     
                                                           |                                       |                           |
         parsl_tasks_description ------>  ParSL_tasks{}----+-> Dep. check ------> ParSL_tasks{} ---+--> ParSL Task/Tasks desc. | umgr.submit_units(RP_units)
                                           +api.submit     | Data management          +dfk.submit  |             |             |
                                                           |                                       |             v             |
                                                           |                                       |     RP Unit/Units desc. --+->   
        ----------------------------------------------------------------------------------------------------------------------------------------------------
    """        
  
    @typeguard.typechecked
    def __init__(self,
                 label: str = 'RADICALExecutor',
                 resource: str = None,
                 login_method: str = None,            #Specify the connection protocol SSH/GISSH/local
                 tasks_pre_exec: Optional[str] = None,      # Specify any requirements that this task needs to run
                 task_process_type: Optional[str] = None, # Specify the type of the process MPI/Non-MPI/rp.Func
                 cores_per_task = 1,
                 managed: bool = True,
                 max_tasks: Union[int, float] = float('inf'),
                 worker_logdir_root: Optional[str] = "."):

        report.header("Initializing RADICALExecutor")
        self.label = label
        self.session = rp.Session(uid=ru.generate_id('parsl.radical_executor.session.%(item_counter)04d',
                                  mode=ru.ID_CUSTOM))
        self.pilot_manager = rp.PilotManager(session=self.session)
        self.unit_manager  = rp.UnitManager(session=self.session)
        self.resource = resource
        self.login_method = login_method
        self.tasks = list()
        self.tasks_pre_exec = tasks_pre_exec
        self.task_process_type = task_process_type
        self.cores_per_task = cores_per_task
        self.managed = managed
        self.max_tasks = max_tasks
        self._task_counter = 0
        self.run_dir = '.'
        self.worker_logdir_root = worker_logdir_root
        #self._executor_bad_state = threading.Event()
        #self._executor_exception = None


    def start(self):
        """Create the Pilot process and pass it.
        """
        report.header("Creating Pilot.....")
        pmgr = self.pilot_manager

        if self.resource is None : logger.error("specify remoute or local resource")


        else : pd_init = {'resource'      : self.resource,
                          'runtime'       : 10,  # pilot runtime (min)
                          'exit_on_error' : True,
                          'project'       : None,
                          'queue'         : None,
                          'access_schema' : self.login_method,
                          'cores'         : 1*self.max_tasks,
                          'gpus'          : 0,}

        pdesc = rp.ComputePilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)

        return pilot


    def submit(self, func, *args, **kwargs):    
        """Submits task/tasks to RADICAL unit_manager.

        Args:
            - func (callable) : Callable function
            - *args (list) : List of arbitrary positional arguments.

        Kwargs:
            - **kwargs (dict) : A dictionary of arbitrary keyword args for func.
        """

        #if self._executor_bad_state.is_set():
        #   raise self._executor_exception

        # here call the start function

        pilot = self.start()
        umgr   = self.unit_manager
        report.header('submit pilots')

        self._task_counter += 1
        task_id = self._task_counter

        report.header('Submitting ParSL %d tasks' % self.max_tasks)

        # Register the ComputePilot in a UnitManager object.
        umgr.add_pilots(pilot)

        report.header('Task name %s ' %((func)))            # THIS NEED TO BE DESERILIZED SOMEHOW
        report.header('Task exe  %s ' %(str(args)))         # Function arguments
        report.header('Task arguments  %s ' %(str(kwargs))) #  {'outputs': ['/home/aymen/stress_ng_0'], 
                                                            #  'stdout': '/home/aymen/stress_out/stress_ng.stdout',
                                                            #  'stderr': '/home/aymen/stress_out/stress_ng.stderr'}
        report.progress_tgt(self.max_tasks, label='create')


        for i in range(0, self.max_tasks):
            
            task = rp.ComputeUnitDescription()
            task.name = func
            task.executable = args
            task.arguments  =  []
            task.stdout = kwargs['stdout']
            task.stderr = kwargs['stderr']
            task.pre_exec   = self.tasks_pre_exec
            task.cpu_processes = self.cores_per_task
            task.cpu_process_type = self.task_process_type
            self.tasks.append(task)
            report.progress()
        
        report.progress_done()
        umgr.submit_units(self.tasks)
        umgr.wait_units()


    def shutdown(self, hub=True, targets='all', block=False):
        """Shutdown the executor, including all RADICAL-Pilot components."""
        self.unit_manager.close()
        self.pilot_manager.close()
        self.session.close(download=True)
        report.header("Attempting RADICALExecutor shutdown")

        return True

    @property
    def scaling_enabled(self) -> bool:
        return False

    def scale_in(self, blocks: int):
        raise NotImplementedError

    def scale_out(self, blocks: int):
        raise NotImplementedError
