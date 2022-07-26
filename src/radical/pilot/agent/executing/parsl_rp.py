"""RADICAL-Executor builds on the RADICAL-Pilot/Parsl
"""
import os
import sys
import time
import parsl
import queue
import inspect
import typeguard

import threading as mt

import radical.pilot as rp
import radical.utils as ru

from radical.pilot import PythonTask

from functools import partial
from concurrent.futures import Future
from typing import Optional, Union

from parsl.utils import RepresentationMixin
from parsl.executors.status_handling import  NoStatusHandlingExecutor

BASH   = 'bash'
PYTHON = 'python'


class RADICALExecutor(NoStatusHandlingExecutor, RepresentationMixin):
    """Executor designed for: executing heterogeneous tasks in terms of
                              type/resource

    The RADICALExecutor system has the following components:

      1. "start"    :creating the RADICAL-executor session and pilot.
      2. "translate":unwrap/identify/ out of parsl task and construct RP task.
      2. "submit"   :translating and submiting Parsl tasks the RADICAL-executor.
      3. "shut_down":shutting down the RADICAL-executor components.

    RADICAL Executor
    ------------------------------------------------------------------------------------------------
             Parsl DFK/dflow               |      Task Translator      |     RP-Client/Task-Manager
    ---------------------------------------|---------------------------|----------------------------
                                           |                           |
    -> Dep. check ------> Parsl_tasks{} <--+--> Parsl Task/func/arg/kwg| tmgr.submit_Tasks(RP_tasks)
     Data management          +dfk.submit  |             |             |
                                           |             v             |
                                           |     RP Task/Tasks desc. --+->
    ------------------------------------------------------------------------------------------------
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
                 project: Optional[str] = " "):

        self._uid               = 'rp.parsl_executor'
        # RP required
        self.project            = project
        self.resource           = resource
        self.login_method       = login_method
        self.partition          = partition
        self.walltime           = walltime

        self.label              = label
        self._task_counter      = 0
        self.future_tasks       = {}

        self.max_tasks          = max_tasks       # Pilot cores
        self.gpus               = gpus
        # Parsl required
        self.managed            = managed
        self.run_dir            = '.'
        self.worker_logdir_root = worker_logdir_root

        self.log    = ru.Logger(name='radical.parsl', level='DEBUG')
        self.prof   = ru.Profiler(name = 'radical.parsl', path = self.run_dir)

        self.session = None
        self.pmgr    = None
        self.tmgr    = None

        # Raptor specific
        self.cfg_file = './raptor.cfg'
        cfg           = ru.Config(cfg=ru.read_json(self.cfg_file))

        self.master      = cfg.master_descr
        self.worker      = cfg.worker_descr
        self.cpn         = cfg.cpn  # cores per node
        self.gpn         = cfg.gpn  # gpus per node
        self.n_masters   = cfg.n_masters  # number of total masters
        self.masters_pn  = cfg.masters_pn  # number of masters per node

        self.pilot_env   = cfg.pilot_env


    def task_state_cb(self, task, state):
        """
        Update the state of Parsl Future tasks
        Based on RP task state
        """
        # FIXME: user might specify task uid as
        # task.uid = 'master...' this migh create
        # a confusion with the raptpor master
        if not task.uid.startswith('master'):

            parsl_task = self.future_tasks[task.uid]

            if state == rp.DONE:
                if task.description['mode'] in [rp.TASK_EXECUTABLE, rp.TASK_EXEC]:
                    parsl_task.set_result(task.stdout)
                    self.log.debug('+ %s: %-10s: %10s: %s', task.uid, task.state,
                                   task.pilot, task.stdout)

                else:
                    parsl_task.set_result(task.return_value)
                    self.log.debug('+ %s: %-10s: %10s: %s', task.uid, task.state,
                                   task.pilot, task.stdout)

            elif state == rp.CANCELED:
                parsl_task.cancel()
                self.log.debug('+ %s: %-10s: %10s: %s', task.uid, task.state,
                               task.pilot, task.stdout)

            elif state == rp.FAILED:
                parsl_task.set_exception(Exception(str(task.stderr)))
                self.log.debug('- %s: %-10s: %10s: %s', task.uid, task.state,
                               task.pilot, task.stderr)


    def start(self):
        """Create the Pilot process and pass it.
        """
        self.log.info("starting RADICALExecutor")
        self.log.info('Parsl: %s', parsl.__version__)
        self.log.info('RADICAL pilot: %s', rp.version)
        self.session = rp.Session(uid=ru.generate_id('parsl.radical.session',
                                                      mode=ru.ID_PRIVATE))

        if self.resource is None : self.log.error("specify remoute or local resource")

        else : pd_init = {'resource'      : self.resource,
                          'runtime'       : self.walltime,
                          'exit_on_error' : True,
                          'project'       : self.project,
                          'queue'         : self.partition,
                          'access_schema' : self.login_method,
                          'cores'         : 1 * self.max_tasks,
                          'gpus'          : self.gpus}
        pd  = rp.PilotDescription(pd_init)
        tds = list()

        for i in range(self.n_masters):
            td = rp.TaskDescription(self.master)
            td.uid            = ru.generate_id('master.%(item_counter)06d',
                                               ru.ID_CUSTOM,
                                               ns=self.session.uid)
            td.arguments      = [self.cfg_file, i]
            td.cpu_threads    = int(self.cpn / self.masters_pn)
            td.input_staging  = [{'source': 'raptor_master.py',
                                  'target': 'raptor_master.py',
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': 'raptor_worker.py',
                                  'target': 'raptor_worker.py',
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': self.cfg_file,
                                  'target': os.path.basename(self.cfg_file),
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS}
                                ]
            tds.append(td)

        self.pmgr = rp.PilotManager(session=self.session)
        self.tmgr = rp.TaskManager(session=self.session)

        # submit pilot(s)
        pilot = self.pmgr.submit_pilots(pd)
        tasks = self.tmgr.submit_tasks(tds)

        # FIXME: master tasks are never checked for, should add a task state
        #        callback to do so.

        pilot.stage_in({'source': ru.which('radical-pilot-hello.sh'),
                        'target': 'radical-pilot-hello.sh',
                        'action': rp.TRANSFER})

        python_v = sys.version.split(' ')[0]
        pilot.prepare_env(env_name='ve_raptor',
                          env_spec={'type'    : self.pilot_env.get('type','virtualenv'),
                                    'version' : self.pilot_env.get('version', python_v),
                                    'path'    : self.pilot_env.get('path', ''),
                                    'pre_exec': self.pilot_env.get('pre_exec', []),
                                    'setup'   : self.pilot_env.get('setup', [])})

        self.tmgr.add_pilots(pilot)
        self.tmgr.register_callback(self.task_state_cb)
        self.log.info('PMGR Is Active submitting tasks now')

        # create a bulking thread to run the actual task submittion to RP in
        # bulks
        self._max_bulk_size = 1024
        self._max_bulk_time =    3        # seconds
        self._min_bulk_time =    0.1      # seconds

        self._bulk_queue    = queue.Queue()
        self._bulk_thread   = mt.Thread(target=self._bulk_collector)

        self._bulk_thread.daemon = True
        self._bulk_thread.start()

        return True


    def unwrap(self, func, args):

        task_type = ''

        # Ignore the resource dict from Parsl
        new_args = list(args)
        new_args.pop(0)
        args = tuple(new_args)

        # remove the remote wrapper from parsl
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__

        # identify the task type
        try:
            # bash and python migh be partial wrapped
            if isinstance(func, partial):
                # @bash_app from parsl
                try:
                    task_type = inspect.getsource(func.args[0]).split('\n')[0]
                    if BASH in task_type:
                        task_type = BASH
                        func = func.args[0]
                    else:
                        task_type = PYTHON

                except Exception:
                    self.log.exception('unwrap failed')

                    return func, args, task_type

            # @python_app from parsl
            else:
                task_type = inspect.getsource(func).split('\n')[0]
                if PYTHON in task_type:
                    task_type = PYTHON
                else:
                    task_type = ''
        except Exception:
            self.log.exception('failed to obtain task type')

        return func, args, task_type


    def task_translate(self, func, args, kwargs):

        task = rp.TaskDescription()
        func, args, task_type = self.unwrap(func, args)

        if BASH in task_type:
          # self.log.debug(BASH)
            if callable(func):
                # These lines of code are from parsl/app/bash.py
                try:
                    # Execute the func to get the command
                    bash_app = func(*args, **kwargs)
                    if not isinstance(bash_app, str):
                        raise ValueError("Expected a str for bash_app cmd, got: %s", type(bash_app))
                except AttributeError as e:
                    raise Exception("failed to obtain bash app cmd") from e

                task.mode       = rp.TASK_EXECUTABLE
                task.scheduler  = None
                task.executable = '/bin/bash'
                task.arguments  = ['-c', bash_app]

                # specifying pre_exec is only for executables
                task.pre_exec = kwargs.get('pre_exec', [])

        elif PYTHON in task_type or not task_type:
          # self.log.debug(PYTHON)
            task.mode       = rp.TASK_FUNCTION
            task_id         = self._task_counter
            task.scheduler  = 'master.%06d' % (task_id % self.n_masters)
            task.function   = PythonTask(func, *args, **kwargs)

        task.stdout           = kwargs.get('stdout', '')
        task.stderr           = kwargs.get('stderr', '')
        task.cpu_threads      = kwargs.get('cpu_threads', 1)
        task.gpu_threads      = kwargs.get('gpu_threads', 0)
        task.cpu_processes    = kwargs.get('cpu_processes', 1)
        task.gpu_processes    = kwargs.get('gpu_processes', 0)
        task.cpu_process_type = kwargs.get('cpu_process_type', '')
        task.gpu_process_type = kwargs.get('gpu_process_type', '')

        return task


    def _bulk_collector(self):

        bulk = list()

        while True:

            now = time.time()  # time of last submission

            # collect tasks for min bulk time
            # NOTE: total collect time could actually be max_time + min_time
            while time.time() - now < self._max_bulk_time:

                try:
                    task = self._bulk_queue.get(block=False, timeout=self._min_bulk_time)
                except queue.Empty:
                    task = None

                if task:
                    bulk.append(task)

                if len(bulk) >= self._max_bulk_size:
                    break

            if bulk:
                self.log.debug('submit bulk: %d', len(bulk))
                self.tmgr.submit_tasks(bulk)
                bulk = list()



    def submit(self, func, *args, **kwargs):
        """
        Submits task/tasks to RADICAL task_manager.

        Args:
            - func (callable) : Callable function
            - *args (list)    : List of arbitrary positional arguments.

        Kwargs:
            - **kwargs (dict) : A dictionary of arbitrary keyword args for func.
        """
        self._task_counter += 1
        task_id = 'task.%d' % self._task_counter
        self.log.debug("got %s from parsl-DFK", task_id)

      # # ----------------------------------------------------------------------
      # # test code: this is the fastest possible executor implementation
      # self.future_tasks[task_id] = Future()
      # self.future_tasks[task_id].set_result(3)
      # return self.future_tasks[task_id]
      # # ----------------------------------------------------------------------

        self.prof.prof(event='trans_start', uid=task_id)
        task = self.task_translate(func, args, kwargs)
        self.prof.prof(event='trans_stop', uid=task_id)

        # assign task id for rp task
        task.uid = task_id

        # set the future with corresponding id
        self.future_tasks[task_id] = Future()

        # push task to rp submit thread
        self._bulk_queue.put(task)

        return self.future_tasks[task_id]


    def _get_job_ids(self):
        return True


    def shutdown(self, hub=True, targets='all', block=False):
        """Shutdown the executor, including all RADICAL-Pilot components."""
        self.log.info("RADICALExecutor shutdown")
        self.session.close(download=True)

        return True

    @property
    def scaling_enabled(self) -> bool:
        return False

    def scale_in(self, blocks: int):
        raise NotImplementedError

    def scale_out(self, blocks: int):
        raise NotImplementedError
