"""RADICAL-Executor builds on the RADICAL-Pilot/ParSL
"""
import re
import sys
import dill
import shlex
import parsl
import inspect
import typeguard

import radical.pilot as rp
import radical.utils as ru

from radical.pilot import PythonTask

from concurrent.futures import Future
from typing import Optional, Union

from colmena.redis.queue import RedisQueue
from parsl.utils import RepresentationMixin
from parsl.executors.status_handling import  NoStatusHandlingExecutor

IDEAL_BSON_SIZE = 48


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
                 max_task_cores: int = 1,
                 cores_per_task: int = 1,
                 gpus: Optional[int]  = 0,
                 worker_logdir_root: Optional[str] = ".",
                 partition : Optional[str] = " ",
                 project: Optional[str] = " ",
                 enable_redis = False,
                 redis_port: int = 59465,
                 redis_host = None):

        self.label              = label
        self.project            = project
        self.resource           = resource
        self.login_method       = login_method
        self.partition          = partition
        self.walltime           = walltime
        self.future_tasks       = {}
        self.managed            = managed
        self.max_tasks          = max_tasks  # Pilot cores
        self.max_task_cores     = max_task_cores  # executor cores
        self.cores_per_task     = cores_per_task  # task cores
        self.gpus               = gpus
        self._task_counter      = 0
        self.run_dir            = '.'
        self.worker_logdir_root = worker_logdir_root
        self.enable_redis       = enable_redis
        self.redis_port         = redis_port
        self.redis_host         = redis_host

        self.logger  = ru.Logger(name='radical.pilot.parsl.executor', level='DEBUG')
        self.report  = ru.Reporter(name='radical.pilot')

        self.session = None
        self.pmgr    = None
        self.tmgr    = None

        # check if we have redis mode enabled
        if self.enable_redis:
            self.redis = RedisQueue(self.redis_host, port = self.redis_port, 
                                    topics = ['rp task queue', 'rp result queue'])

    def get_redis_task(self):
        '''
        Pull a result object from redis queue
        '''
        if self.enable_redis:
            # make sure we are connected to redis
            assert(self.redis.is_connected)
            message = self.redis.get(topic = 'rp result queue')

            if message:
                self.logger.debug('pulled result from redis')
                result = eval(message[1])
                try:
                    STDOUT = dill.loads(result)
                except Exception as e:
                    self.logger.error(str(e))
            return STDOUT

    def put_redis_task(self, tu):
        '''
        Push a result object to redis queue
        '''
        if self.enable_redis:
            # make sure we are connected to redis
            assert(self.redis.is_connected)
            source_code = str(dill.dumps(tu))
            self.redis.put(source_code, topic = 'rp task queue')
            self.logger.debug('task pushed to redis')

    def task_state_cb(self, task, state):
        """
        Update the state of Parsl Future tasks
        Based on RP task state
        """
        parsl_task = self.future_tasks[task.uid]
        STDOUT = task.stdout
        if state == rp.DONE:
            if task.name == 'colmena':
                self.logger.debug('got a colmena result')
                try:
                    STDOUT = self.get_redis_task()
                except Exception as e:
                    self.logger.debug(e)
            else:
                pass
            parsl_task.set_result(STDOUT)
            print('\t+ %s: %-10s: %10s'
                  % (task.uid, task.state, task.pilot))

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
        if self.resource is None : self.logger.error("specify remoute or local resource")

        else : pd_init = {'resource'      : self.resource,
                          'runtime'       : self.walltime,
                          'exit_on_error' : True,
                          'project'       : self.project,
                          'queue'         : self.partition,
                          'access_schema' : self.login_method,
                          'cores'         : 1 * self.max_tasks,
                          'max_task_cores': self.max_task_cores,
                          'gpus'          : self.gpus,}

        pdesc = rp.PilotDescription(pd_init)

        self.pmgr = rp.PilotManager(session=self.session)
        self.tmgr = rp.TaskManager(session=self.session)

        # submit pilot(s)
        pilot = self.pmgr.submit_pilots(pdesc)

        self.tmgr.add_pilots(pilot)
        self.tmgr.register_callback(self.task_state_cb)
        self.report.header('PMGR Is Active submitting tasks now')

        if self.enable_redis:
            self.redis.connect()

        return True

    def unwrap(self, func):
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
        return func



    def task_translate(self, func, args, kwargs):

        try:
            task_type = inspect.getsource(func).split('\n')[0]
        except:
            task_type = None

        if task_type:
            code  = None

            if task_type.startswith('@bash_app'):
                source_code = inspect.getsource(func).split('\n')[2].split('return')[1]
                temp        = ' '.join(shlex.quote(arg) for arg in (shlex.split(source_code,
                                                                    comments=True, posix=True)))
                task_exe    = re.findall(r"'(.*?).format", temp,re.DOTALL)[0]
                if 'exe' in kwargs:
                    code = "{0} {1}".format(kwargs['exe'], task_exe)
                else:
                    code = task_exe

            elif task_type.startswith('@python_app'):
                # We ignore the resource dict from Parsl
                new_args = list(args)
                new_args.pop(0)
                args = tuple(new_args)
                code  = PythonTask(self.unwrap(func), *args, **kwargs) 


            tu =  {"source_code": code,
                   "name"       : func.__name__,
                   "args"       : [],
                   "kwargs"     : kwargs,
                   "pre_exec"   : None if 'pre_exec' not in kwargs else kwargs['pre_exec'],
                   "ptype"      : rp.FUNC if 'ptype' not in kwargs else kwargs['ptype'],
                   "nproc"      : 1 if 'nproc' not in kwargs else kwargs['nproc'],
                   "nthrd"      : 1 if 'nthrd' not in kwargs else kwargs['nthrd'],
                   "ngpus"      : 0 if 'ngpus' not in kwargs else kwargs['ngpus']}


        else:
            rp_func = self.unwrap(func)
            # We ignore the resource dict from Parsl
            new_args = list(args)
            new_args.pop(0)
            args = tuple(new_args)

            # MongoDB can not handle it, we will pull it from
            # the Redis server (agent side)
            name = func.__name__
            if sys.getsizeof(args) >= IDEAL_BSON_SIZE:
                name = 'colmena'

            tu = {"source_code" : PythonTask(rp_func, *args, **kwargs),
                  "name"       : name,
                  "args"       : [],
                  "kwargs"     : kwargs,
                  "pre_exec"   : None if 'pre_exec' not in kwargs else kwargs['pre_exec'],
                  "ptype"      : rp.MPI_FUNC if 'mpi' in self.resource else rp.FUNC,
                  "nproc"      : self.cores_per_task,
                  "nthrd"      : 1 if 'nthrd' not in kwargs else kwargs['nthrd'],
                  "ngpus"      : 0 if 'ngpus' not in kwargs else kwargs['ngpus']}

        return tu

    def submit(self, func, *args, **kwargs):
        """
        Submits task/tasks to RADICAL task_manager.

        Args:
            - func (callable) : Callable function
            - *args (list) : List of arbitrary positional arguments.

        Kwargs:
            - **kwargs (dict) : A dictionary of arbitrary keyword args for func.
        """
        self.logger.debug("Got a task from the parsl.dfk")

        self._task_counter += 1
        task_id = str(self._task_counter)
        self.future_tasks[task_id] = Future()
        tu = self.task_translate(func, args, kwargs)            

        try:
            self.report.progress_tgt(self._task_counter, label='create')

            task                  = rp.TaskDescription()
            task.uid              = task_id
            task.name             = tu['name']
            task.pre_exec         = tu['pre_exec']
            task.executable       = tu['source_code']
            task.arguments        = tu['args']
            task.cpu_processes    = tu['nproc']
            task.cpu_threads      = tu['nthrd']
            task.cpu_process_type = tu['ptype']
            task.gpu_processes    = tu['ngpus']
            task.gpu_process_type = None
            self.report.progress()

            if tu['name'] == 'colmena':
                self.put_redis_task(tu['source_code'])
                tu['source_code']['args'] = ()

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

