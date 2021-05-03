"""RADICAL-Executor builds on the RADICAL-Pilot/ParSL
"""
import os
import re
import shlex
import parsl
import time
import queue
import pickle
import logging
import typeguard
import threading
import inspect

import radical.pilot as rp
import radical.utils as ru

from radical.pilot import PythonTask

from concurrent.futures import Future

from multiprocessing import Process, Queue
from typing import Any, Dict, List, Optional, Tuple, Union

from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.errors import *
from parsl.utils import RepresentationMixin
from parsl.executors.status_handling import  NoStatusHandlingExecutor



class RADICALExecutor(NoStatusHandlingExecutor, RepresentationMixin):
    """Executor designed for cluster-scale

    The RADICALExecutor system has the following components:

      1. "start" resposnible for creating the RADICAL-executor session and pilot.
      2. "submit" resposnible for translating and submiting ParSL tasks the RADICAL-executor.
      3. "shut_down"  resposnible for shutting down the RADICAL-executor components.

    Here is a diagram

    .. code:: python
                                                                                                                     RADICAL Executor
        ----------------------------------------------------------------------------------------------------------------------------------------------------
                        ParSL API                          |         ParSL DFK/dflow               |      Task Translator      |     RP-Client/Task-Manager
        ---------------------------------------------------|---------------------------------------|---------------------------|----------------------------                                                     
                                                           |                                       |                           |
         parsl_tasks_description ------>  ParSL_tasks{}----+-> Dep. check ------> ParSL_tasks{} <--+--> ParSL Task/Tasks desc. | tmgr.submit_Tasks(RP_tasks)
                                           +api.submit     | Data management          +dfk.submit  |             |             |
                                                           |                                       |             v             |
                                                           |                                       |     RP Task/Tasks desc. --+->   
        ----------------------------------------------------------------------------------------------------------------------------------------------------
    """        
  
    @typeguard.typechecked
    def __init__(self,
                 label: str = 'RADICALExecutor',
                 resource: str = None,
                 login_method: str = None,
                 walltime: int = None,
                 managed: bool = True,
                 max_tasks: Union[int, float] = float('inf'),
                 gpus: Optional[int]  = 0,
                 worker_logdir_root: Optional[str] = ".",
                 partition : Optional[str] = " ",
                 project: Optional[str] = " ",):

        self.label              = label
        self.project            = project
        self.resource           = resource
        self.login_method       = login_method
        self.partition          = partition
        self.walltime           = walltime
        self.future_tasks       = {}
        self.managed            = managed
        self.max_tasks          = max_tasks
        self.gpus               = gpus
        self._task_counter      = 0
        self.run_dir            = '.'
        self.worker_logdir_root = worker_logdir_root

        self.logger  = ru.Logger(name='radical.pilot.parsl.executor', level='DEBUG')
        self.report  = ru.Reporter(name='radical.pilot')
        self.session = rp.Session(uid=ru.generate_id('parsl.radical_executor.session',
                                                      mode=ru.ID_PRIVATE))

        self.report.title('RP version %s :' % rp.version)
        self.report.header("Initializing RADICALExecutor with ParSL version %s :" % parsl.__version__)
        self.pmgr    = rp.PilotManager(session=self.session)
        self.tmgr    = rp.TaskManager(session=self.session)
        
    def task_state_cb(self, task, state):

        """
        Update the state of Parsl Future tasks
        Based on RP task state
        """
        parsl_task = self.future_tasks[task.name]
        if state == rp.DONE:
            parsl_task.set_result(task.stdout)
            print('\t+ %s: %-10s: %10s: %s'
                  % (task.uid, task.state, task.pilot, task.stdout))

        if state == rp.CANCELED:
            parsl_task.cancel()
            print('\t+ %s: %-10s: %10s: %s'
                  % (task.uid, task.state, task.pilot, task.stdout))
        if state == rp.FAILED:
            parsl_task.set_exception(Exception(str(task.stderr)))
            print('\t- %s: %-10s: %10s: %s'
                  % (task.uid, task.state, task.pilot, task.stderr))


    def start(self):
        """Create the Pilot process and pass it.
        """
        if self.resource is None : self.logger.error("specify remoute or local resource")

        else : pd_init = {'resource'      : self.resource,
                          'runtime'       : self.walltime,
                          'exit_on_error' : True,
                          'project'       : self.project,
                          'queue'         : self.partition,
                          'access_schema' : self.login_method,
                          'cores'         : 1*self.max_tasks,
                          'gpus'          : self.gpus,}

        pdesc = rp.PilotDescription(pd_init)
        pilot = self.pmgr.submit_pilots(pdesc)
        self.tmgr.add_pilots(pilot)
        pilot.wait(state=rp.PMGR_ACTIVE)
        time.sleep(60)
        self.report.header('PMGR Is Active submitting tasks now')

        return True
    
    def unwrap(self, func):
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
        return func



    def task_translate(self, func, args, kwargs):

        task_type = inspect.getsource(func).split('\n')[0]
        if task_type.startswith('@bash_app'):
            source_code = inspect.getsource(func).split('\n')[2].split('return')[1]
            temp        = ' '.join(shlex.quote(arg) for arg in (shlex.split(source_code,
                                                                            comments=True, posix=True)))
            task_exe    = re.findall(r"'(.*?).format", temp,re.DOTALL)[0]
            if 'exe' in kwargs:
                code = "{0} {1}".format(kwargs['exe'], task_exe)
            else:
                code = task_exe

            cu =  {"source_code": code,
                   "name"       : func.__name__,
                   "args"       : None,
                   "kwargs"     : kwargs,
                   "pre_exec"   : None if 'pre_exec' not in kwargs else kwargs['pre_exec'],
                   "ptype"      : None if 'ptype' not in kwargs else kwargs['ptype'],
                   "nproc"      : 1 if 'nproc' not in kwargs else kwargs['nproc'],
                   "nthrd"      : 1 if 'nthrd' not in kwargs else kwargs['nthrd'],
                   "ngpus"      : 0 if 'ngpus' not in kwargs else kwargs['ngpus']}


        elif task_type.startswith('@python_app'):

            rp_func  = self.unwrap(func)

            # We ignore the resource dict from Parsl
            new_args = list(args)
            new_args.pop(0)      
            args = tuple(new_args)

            cu = {"source_code": PythonTask(rp_func, *args, **kwargs),
                  "name"       : func.__name__,
                  "args"       : [],
                  "kwargs"     : kwargs,
                  "pre_exec"   : None if 'pre_exec' not in kwargs else kwargs['pre_exec'],
                  "ptype"      : rp.FUNC if 'ptype' not in kwargs else rp.MPI_FUNC,
                  "nproc"      : 1 if 'nproc' not in kwargs else kwargs['nproc'],
                  "nthrd"      : 1 if 'nthrd' not in kwargs else kwargs['nthrd'],
                  "ngpus"      : 0 if 'ngpus' not in kwargs else kwargs['ngpus']}

        else:
            pass
        return cu

    def submit(self, func, *args, **kwargs):
        """
        Submits task/tasks to RADICAL task_manager.

        Args:
            - func (callable) : Callable function
            - *args (list) : List of arbitrary positional arguments.

        Kwargs:
            - **kwargs (dict) : A dictionary of arbitrary keyword args for func.
        """
        self.logger.debug("Got a task from the parsl.dataflow.dflow")

        self._task_counter += 1
        task_id = str(self._task_counter)
        self.future_tasks[task_id] = Future()
        tu = self.task_translate(func, args, kwargs)

        try:
            self.tmgr.register_callback(self.task_state_cb) 
            self.report.progress_tgt(self._task_counter, label='create')

            task                  = rp.TaskDescription()
            task.name             = task_id
            task.pre_exec         = tu['pre_exec']
            task.executable       = tu['source_code']
            task.arguments        = tu['args']
            task.cpu_processes    = tu['nproc']
            task.cpu_threads      = tu['nthrd']
            task.cpu_process_type = tu['ptype']
            task.gpu_processes    = tu['ngpus']
            task.gpu_process_type = None
            self.report.progress()
            self.tmgr.submit_tasks(task)

        except Exception as e:
            # Something unexpected happened in the pilot code above
            self.report.error('caught Exception: %s\n' % e)
            ru.print_exception_trace()
            raise

        except (KeyboardInterrupt, SystemExit):
            ru.print_exception_trace()
            self.report.warn('exit requested\n')

        return self.future_tasks[task_id]


    def _get_job_ids(self):
        return True


    def shutdown(self, hub=True, targets='all', block=False):
        """Shutdown the executor, including all RADICAL-Pilot components."""
        self.report.progress_done()
        self.session.close(download=True)
        self.report.header("Attempting RADICALExecutor shutdown")

        return True

    @property
    def scaling_enabled(self) -> bool:
        return False

    def scale_in(self, blocks: int):
        raise NotImplementedError

    def scale_out(self, blocks: int):
        raise NotImplementedError

