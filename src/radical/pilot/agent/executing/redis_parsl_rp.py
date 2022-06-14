<<<<<<< HEAD
import dill
import radical.utils as ru
import radical.pilot as rp
=======
import time
import redis
import radical.utils as ru
import radical.pilot as rp
import radical.pilot.utils as rpu
>>>>>>> d3203b568a8cd974f6094498114c056356f8bc5f

from functools import partial
from .parsl_rp import BASH, PYTHON, RADICALExecutor
from colmena.models import Result
from colmena.models import ExecutableTask
<<<<<<< HEAD
from colmena.redis.queue import RedisQueue
=======
>>>>>>> d3203b568a8cd974f6094498114c056356f8bc5f

COLMENA = 'colmena'

class RedisRadicalExecutor(RADICALExecutor):

    def __init__(self, label="RedisRadicalExecutor", resource=None, login_method=None,
                       walltime=None, managed=True, max_tasks=float('inf'),
<<<<<<< HEAD
                       cores_per_task=1, gpus=0, worker_logdir_root=".",
                       partition=" ", project=" ", enable_redis=False,
                       redis_port=6379, redis_host:str ='127.0.0.1',
                       redis_pass: str=None):

        # Needed by Colmena
        self.enable_redis = enable_redis
=======
                       gpus=0, worker_logdir_root=".", partition=" ",
                       project=" ", redis_port=6379, redis_host:str ='127.0.0.1',
                       redis_pass: str=None):

        # Needed by Colmena
>>>>>>> d3203b568a8cd974f6094498114c056356f8bc5f
        self.redis_port   = redis_port
        self.redis_host   = redis_host
        self.redis_pass   = redis_pass

        super().__init__(label, resource, login_method, walltime, managed, max_tasks,
<<<<<<< HEAD
                         cores_per_task, gpus, worker_logdir_root, partition,
                         project)

        # check if we have redis mode enabled and connect
        if self.enable_redis:
            self.redis = RedisQueue(self.redis_host, port = self.redis_port, 
                                    password = self.redis_pass, topics = \
                                    ['rp task queue', 'rp result queue'])
            self.redis.connect()

        cfg = ru.read_json("raptor.cfg")
        cfg["redis"] = {"host": self.redis_host, 
                        "port": self.redis_port,
                        "pass": self.redis_pass}
=======
                         gpus, worker_logdir_root, partition, project)

        self.redis = redis.Redis(host=self.redis_host, port = self.redis_port, 
                                                   password = self.redis_pass)
        assert(self.redis.ping())

        cfg = ru.read_json("raptor.cfg")
        cfg["redis"] = {"host": self.redis_host,
                        "port": self.redis_port,
                        "pass": self.redis_pass}
        # custom redis worker for colmena
        cfg["worker_descr"]["worker_file"]  = "./raptor_worker.py"
>>>>>>> d3203b568a8cd974f6094498114c056356f8bc5f
        cfg["worker_descr"]["worker_class"] = "WorkerRedis"

        ru.write_json(cfg, "raptor.cfg")

<<<<<<< HEAD
    def get_redis_task(self):
        '''
        Pull a result object from redis queue
        '''
        if self.enable_redis:
            # make sure we are connected to redis
            assert(self.redis.is_connected)
            message = self.redis.get(topic = 'rp result queue')

            if message:
                self.log.debug('get_task_from_redis')
                result = eval(message[1])
                try:
                    stdout = dill.loads(result)
                except Exception as e:
                    self.log.error(str(e))
            return stdout
=======
    def get_redis_task(self, task_id):
        '''
        Pull a result object from redis queue
        '''
        key = 'result:{0}'.format(str(task_id))
        if self.redis.exists(key):
            msg = self.redis.get(key)
            task = eval(msg.decode())
            if task['uid'] == task_id:
                self.log.debug('key found and matched')
                retv = rpu.deserialize_obj(eval(task['return_value']))
                self.redis.delete(key)
                return retv
            else:
                raise('inconsistent rp task and redis task')

>>>>>>> d3203b568a8cd974f6094498114c056356f8bc5f

    def put_redis_task(self, task):
        '''
        Push a result object to redis queue
        '''
<<<<<<< HEAD
        if self.enable_redis:
            # make sure we are connected to redis
            assert(self.redis.is_connected)
            message = str(task)
            self.redis.put(message, topic = 'rp task queue')
            self.log.debug('send_task_to_redis')
=======
        key = 'task:{0}'.format(str(task.uid))
        self.redis.set(key, str(task))
        self.log.debug('send_task_to_redis')
>>>>>>> d3203b568a8cd974f6094498114c056356f8bc5f


    def task_state_cb(self, task, state):
        """
        Update the state of Parsl Future tasks
        Based on RP task state
        """
        if not task.uid.startswith('master'):
            parsl_task = self.future_tasks[task.uid]
            if state == rp.DONE and task.name == COLMENA:
<<<<<<< HEAD
                self.log.debug('recv_colmena_result')
                try:
                    stdout = self.get_redis_task()
                    parsl_task.set_result(stdout)
                except Exception as e:
                    self.log.debug(e)
=======
                try:
                    stdout = self.get_redis_task(task.uid)
                    parsl_task.set_result(stdout)
                    self.log.debug('recv_colmena_result')
                except Exception as e:
                    self.log.error(e)
                    raise e
>>>>>>> d3203b568a8cd974f6094498114c056356f8bc5f
            else:
                return super().task_state_cb(task, state)


    # -------------------------------------------------------------------------
    #
    def unwrap(self, func, args):

        task_type = ''

        # Ignore the resource dict from Parsl
        new_args = list(args)
        new_args.pop(0)
        args = tuple(new_args)

        # remove the remote wrapper from parsl
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            # Colmena/bash and python migh be partial wrapped
            if isinstance(func, partial):
                self.log.debug('COL_TASK_PARTIAL')
                # type bash/python colmena task
                if isinstance(func.args[0], ExecutableTask):
                    self.log.debug('COL_TASK_EXECUTABLETASK')
                    # we can only check via name now as dfk not returning 
                    # app type with base class
                    if '_preprocess' or '_postprocess' in func.__name__:
                        self.log.debug('COL_TASK_PYTHON')
<<<<<<< HEAD
=======
                        self.log.debug(str(func.__name__))
>>>>>>> d3203b568a8cd974f6094498114c056356f8bc5f
                        task_type = PYTHON
                        return func, args, task_type

                if isinstance(func.args[0], partial):
                    if '_execute_execute' in func.args[0].func.__name__:
                        self.log.debug('COL_TASK_BASH')
                        task_type = BASH
                        return func.args[0], args, task_type

    def task_translate(self, func, args, kwargs):
        # Colmena task if the task args is type Result
        task = super().task_translate(func, args, kwargs)
<<<<<<< HEAD
=======
        task.uid = str(self._task_counter)
>>>>>>> d3203b568a8cd974f6094498114c056356f8bc5f
        if len(args) > 0:
            for arg in args: 
                if isinstance(arg, Result):
                    self.log.debug('recv_colmena_task')
                    task.name = COLMENA

<<<<<<< HEAD
        if task.pyfunction:
            self.put_redis_task(task.pyfunction)
            task.pyfunction = 'redis_func'

        return task
=======
        if task.function:
            self.put_redis_task(task)
            task.function = 'redis_func'

        return task


    def shutdown(self, hub=True, targets='all', block=False):
        """Shutdown the executor, including all RADICAL-Pilot
           components and redis instances
        """
        super().shutdown(hub=True, targets='all', block=False)
        self.redis.flushall()
        return True

>>>>>>> d3203b568a8cd974f6094498114c056356f8bc5f
