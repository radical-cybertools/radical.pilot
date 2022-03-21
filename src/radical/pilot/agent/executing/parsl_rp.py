"""RADICAL-Executor builds on the RADICAL-Pilot/Parsl
"""
import os
import dill
import shlex
import parsl
import inspect
import typeguard

import radical.pilot as rp
import radical.utils as ru

from radical.pilot import PythonTask

from functools import partial
from concurrent.futures import Future
from typing import Optional, Union

from colmena.models import Result
from colmena.models import ExecutableTask
from colmena.redis.queue import RedisQueue

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
    ------------------------------------------------------------------------------------------------
             Parsl DFK/dflow               |      Task Translator      |     RP-Client/Task-Manager
    ---------------------------------------|---------------------------|----------------------------                                                     
                                           |                           |
    -> Dep. check ------> Parsl_tasks{} <--+--> Parsl Task/Tasks desc. | tmgr.submit_Tasks(RP_tasks)
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
                 cores_per_task: int = 1,
                 gpus: Optional[int]  = 0,
                 worker_logdir_root: Optional[str] = ".",
                 partition : Optional[str] = " ",
                 project: Optional[str] = " ",
                 enable_redis = False,
                 redis_port: int = 59465,
                 redis_host = None):

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
        self.cores_per_task     = cores_per_task  # task cores
        self.gpus               = gpus
        # Parsl required
        self.managed            = managed
        self.run_dir            = '.'
        self.worker_logdir_root = worker_logdir_root

        # Needed by Colmena
        self.enable_redis       = enable_redis
        self.redis_port         = redis_port
        self.redis_host         = redis_host

        self.log    = ru.Logger(name='radical.parsl', level='DEBUG')
        self.prof   = ru.Profiler(name = 'radical.parsl', path = self.run_dir)
        self.report = ru.Reporter(name='radical.pilot')

        self.session = None
        self.pmgr    = None
        self.tmgr    = None

        # check if we have redis mode enabled
        if self.enable_redis:
            self.redis = RedisQueue(self.redis_host, port = self.redis_port, 
                                    topics = ['rp task queue', 'rp result queue'])
        # Raptor specific
        self.cfg_file = './raptor.cfg'
        cfg         = ru.Config(cfg=ru.read_json(self.cfg_file))

        self.master      = cfg.master_descr
        self.worker      = cfg.worker_descr
        self.cpn         = cfg.cpn  # cores per node
        self.gpn         = cfg.gpn  # gpus per node
        self.n_masters   = cfg.n_masters  # number of total masters
        self.n_workers   = cfg.n_workers  # number of workers per node
        self.masters_pn  = cfg.masters_pn  # number of masters per node
        self.nodes_pw    = cfg.nodes_pw    # number of nodes per worker
        self.nodes_rp    = cfg.nodes_rp   # number of total nodes
        self.nodes_agent = cfg.nodes_agent  # number of nodes per agent

    def get_redis_task(self):
        '''
        Pull a result object from redis queue
        '''
        if self.enable_redis:
            # make sure we are connected to redis
            assert(self.redis.is_connected)
            message = self.redis.get(topic = 'rp result queue')

            if message:
                self.log.debug('pulled result from redis')
                result = eval(message[1])
                try:
                    STDOUT = dill.loads(result)
                except Exception as e:
                    self.log.error(str(e))
            return STDOUT

    def put_redis_task(self, task):
        '''
        Push a result object to redis queue
        '''
        if self.enable_redis:
            # make sure we are connected to redis
            assert(self.redis.is_connected)
            source_code = str(task)
            self.redis.put(source_code, topic = 'rp task queue')
            self.log.debug('task pushed to redis')

    def task_state_cb(self, task, state):
        """
        Update the state of Parsl Future tasks
        Based on RP task state
        """
        if not task.uid.startswith('master'):
            parsl_task = self.future_tasks[task.uid]

            STDOUT = task.stdout
            if state == rp.DONE:
                if task.name == 'colmena':
                    self.log.debug('got a colmena result')
                    try:
                        STDOUT = self.get_redis_task()
                    except Exception as e:
                        self.log.debug(e)
                else:
                    pass
                parsl_task.set_result(STDOUT)
                self.log.debug(STDOUT)
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
        self.report.header("starting RADICALExecutor")
        self.report.header('Parsl: %s' % parsl.__version__)
        self.report.header('RADICAL pilot: %s' % rp.version)
        self.session = rp.Session(uid=ru.generate_id('parsl.radical.session',
                                                      mode=ru.ID_PRIVATE))
        pilot_redis_url = None
        if self.enable_redis and self.redis_host and self.redis_port:
            pilot_redis_url = '{}:{}'.format(self.redis_host, self.redis_port)

        if self.resource is None : self.log.error("specify remoute or local resource")

        else : pd_init = {'resource'      : self.resource,
                          'runtime'       : self.walltime,
                          'exit_on_error' : True,
                          'project'       : self.project,
                          'queue'         : self.partition,
                          'access_schema' : self.login_method,
                          'cores'         : 1 * self.max_tasks,
                          'gpus'          : self.gpus,
                          'redis_link'    : pilot_redis_url}
        pd = rp.PilotDescription(pd_init)

        pd.cores   = self.n_masters * (self.cpn / self.masters_pn)
        pd.gpus    = 0

        pd.cores  += self.n_masters * self.n_workers * self.cpn * self.nodes_pw
        pd.gpus   += self.n_masters * self.n_workers * self.gpn * self.nodes_pw

        pd.cores  += self.nodes_agent * self.cpn
        pd.gpus   += self.nodes_agent * self.gpn

        pd.cores  += self.nodes_rp * self.cpn
        pd.gpus   += self.nodes_rp * self.gpn

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
        task  = self.tmgr.submit_tasks(tds)

        pilot.stage_in({'source': ru.which('radical-pilot-hello.sh'),
                        'target': 'radical-pilot-hello.sh',
                        'action': rp.TRANSFER})
        pilot.prepare_env(env_name='ve_raptor',
                          env_spec={'type'   : 'virtualenv',
                                    'version': '3.8',
                                    # 'path'   : '',
                                     'setup'  : ['$HOME/radical.utils/',
                                                 '$HOME/radical.pilot/',
                                                 '$HOME/colmena/']})
        if self.enable_redis:
            self.redis.connect()

        self.tmgr.add_pilots(pilot)
        self.tmgr.register_callback(self.task_state_cb)
        self.report.header('PMGR Is Active submitting tasks now')

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

        # ideftify the task type
        try:
            # Colmena/bash and python migh be partial wrapped
            if isinstance(func, partial):

                # type bash/python colmena task
                if isinstance(func.args[0], ExecutableTask):

                    # we can only check via name now as dfk not returning 
                    # app type with base class
                    if '_preprocess' or '_postprocess' in func.__name__:
                        task_type = 'python'
                        return func, args, task_type
                    
                if isinstance(func.args[0], partial):
                    if '_execute_execute' in func.args[0].func.__name__:
                        task_type = 'bash'
                        return func.args[0], args, task_type

                # type python (colmena task or non colmena task) or bash_app
                else:
                    # @bash_app from parsl
                    try:
                        task_type = inspect.getsource(func.args[0]).split('\n')[0]
                        if 'bash' in task_type:
                            task_type = 'bash'
                            func = func.args[0]
                        else:
                            task_type = 'python'

                    except Exception as e:
                        self.report.header(str(e))

                    return func, args, task_type

            # @python_app from parsl
            else:
                task_type = inspect.getsource(func).split('\n')[0]
                if 'python' in task_type:
                    task_type = 'python'
                else:
                    task_type = ''
        except Exception as e:
            self.report.header('failed to obtain task type: %s', e)

        return func, args, task_type


    def task_translate(self, func, args, kwargs):

        task = rp.TaskDescription()
        func, args, task_type = self.unwrap(func, args)

        if 'bash' in task_type:
            self.log.debug('bash app')
            if callable(func):
                # These lines of code are from parsl/app/bash.py
                try:
                    # Execute the func to get the command
                    bash_app = func(*args, **kwargs)
                    if not isinstance(bash_app, str):
                        raise ValueError("Expected a str for bash_app cmd, got: %s", type(bash_app))

                except AttributeError as e:
                    raise Exception("failed to obtain bash app cmd") from e

                task.mode = rp.TASK_EXECUTABLE

                if 'mpirun' in bash_app:
                    bash_app              = shlex.split(bash_app)
                    task.executable       = bash_app[3]
                    task.arguments        = eval(bash_app[4:][0])
                    task.cpu_processes    = eval(bash_app[2])
                    task.cpu_process_type = rp.MPI
                else:
                    task.executable = bash_app

        elif 'python' in task_type or not task_type:
            self.log.debug('python app')

            # Colmena task if the task args is type Result
            if len(args) > 0:
                for arg in args: 
                    if isinstance(arg, Result):
                        self.log.debug('got colmena args')
                        task.name = 'colmena'

            code = PythonTask(func, *args, **kwargs)

            task.mode       = rp.TASK_PY_FUNCTION
            task.pyfunction = code

        task.stdout            = "" if 'stdout' not in kwargs else kwargs['stdout']
        task.stderr            = "" if 'stderr' not in kwargs else kwargs['stderr']
        #task.pre_exec         = [] if 'pre_exec' not in kwargs else kwargs['pre_exec']
        #task.cpu_process_type = None if 'ptype' not in kwargs else kwargs['ptype']
        #task.cpu_processes    = 1 if 'nproc' not in kwargs else kwargs['nproc']
        #task.cpu_threads      = 0 if 'nthrd' not in kwargs else kwargs['nthrd']
        #task.gpu_processes    = 0 if 'ngpus' not in kwargs else kwargs['ngpus']
        #task.gpu_process_type = None

        return task


    def submit(self, func, *args, **kwargs):
        """
        Submits task/tasks to RADICAL task_manager.

        Args:
            - func (callable) : Callable function
            - *args (list) : List of arbitrary positional arguments.

        Kwargs:
            - **kwargs (dict) : A dictionary of arbitrary keyword args for func.
        """
        self.log.debug("Got a task from the parsl.dfk")
        self._task_counter += 1
        task_id = str(self._task_counter)

        try:
            #self.report.progress_tgt(self._task_counter, label='create')

            self.prof.prof(event= 'trans_start', uid=self._uid)
            task = self.task_translate(func, args, kwargs)
            self.prof.prof(event= 'trans_stop', uid=self._uid)

            if task.mode == rp.TASK_EXECUTABLE:
                task.scheduler = None
            else:
                task.scheduler = 'master.%06d' % (self._task_counter % self.n_masters)

            self.report.progress()

            if task.name == 'colmena':
                if task.pyfunction:
                    self.put_redis_task(task.pyfunction)
                    task.pyfunction = 'redis_func'
                else:
                    self.put_redis_task(task.executable)
                    task.executable = 'redis_exec'

            # we assign a task id for rp task
            task.uid = task_id

            # set the future with corresponding id
            self.future_tasks[task_id] = Future()

            # submit the task to rp
            self.tmgr.submit_tasks(task)

            return self.future_tasks[task_id]


        except Exception as e:
            # Something unexpected happened in the pilot code above
            self.report.error('caught Exception: %s\n' % e)
            ru.print_exception_trace()
            raise

        except (KeyboardInterrupt, SystemExit):
            ru.print_exception_trace()
            self.report.warn('exit requested\n')


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
