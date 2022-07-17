import dill
import redis
import radical.utils as ru
import radical.pilot as rp
import radical.pilot.utils as rpu

from functools import partial
from .parsl_rp import BASH, PYTHON, RADICALExecutor
from colmena.models import Result
from colmena.models import ExecutableTask

COLMENA = 'colmena'

class RedisRadicalExecutor(RADICALExecutor):

    def __init__(self, label="RedisRadicalExecutor", resource=None, login_method=None,
                       walltime=None, managed=True, max_tasks=float('inf'),
                       gpus=0, worker_logdir_root=".", partition=" ",
                       project=" ", redis_port=6379, redis_host:str ='127.0.0.1',
                       redis_pass: str=None):

        # Needed by Colmena
        self.redis_port   = redis_port
        self.redis_host   = redis_host
        self.redis_pass   = redis_pass

        super().__init__(label, resource, login_method, walltime, managed, max_tasks,
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
        cfg["worker_descr"]["worker_class"] = "WorkerRedis"

        ru.write_json(cfg, "raptor.cfg")

    def get_redis_task(self, task_id):
        '''
        Pull a result object from redis queue
        '''
        key = 'result:{0}'.format(str(task_id))
        if self.redis.exists(key):
            msg = self.redis.get(key)
            task = eval(msg.decode())
            if task['uid'] == task_id:
                self.log.debug('got redis-task %s', task['uid'])
                retv = rpu.deserialize_obj(eval(task['return_value']))
                return retv
            else:
                raise('inconsistent rp task and redis task')


    def put_redis_task(self, task):
        '''
        Push a result object to redis queue
        '''
        key = 'task:{0}'.format(str(task.uid))
        self.redis.set(key, str(task))
        self.log.debug('put redis-task %s', task.uid)


    def task_state_cb(self, task, state):
        """
        Update the state of Parsl Future tasks
        Based on RP task state
        """
        if not task.uid.startswith('master'):
            parsl_task = self.future_tasks[task.uid]
            if state == rp.DONE and task.name == COLMENA:
                try:
                    stdout = self.get_redis_task(task.uid)
                    parsl_task.set_result(stdout)
                    self.log.debug('got colmena-result of %s', task.uid)
                except Exception as e:
                    self.log.error(e)
                    raise e
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
                self.log.debug('colmena PartialTask')
                # type bash/python colmena task
                if isinstance(func.args[0], ExecutableTask):
                    self.log.debug('colmena ExecutableTask')
                    # we can only check via name now as dfk not returning
                    # app type with base class
                    if '_preprocess' or '_postprocess' in func.__name__:
                        self.log.debug('colmena PythonTask: %s', func.__name__)
                        task_type = PYTHON
                        return func, args, task_type

                if isinstance(func.args[0], partial):
                    if '_execute_execute' in func.args[0].func.__name__:
                        self.log.debug('colmena BashTask')
                        task_type = BASH
                        return func.args[0], args, task_type


    # -------------------------------------------------------------------------
    #
    def task_translate(self, func, args, kwargs):

        # Colmena task if the task args is type Result
        task = super().task_translate(func, args, kwargs)
        task.uid = str(self._task_counter)
        if len(args) > 0:
            for arg in args:
                if isinstance(arg, Result):
                    self.log.debug('got colmena-task %s', task.uid)
                    task.name = COLMENA

        if task.function:
            self.put_redis_task(task)
            task.function = 'redis_func'

        return task


    # -------------------------------------------------------------------------
    #
    def shutdown(self, hub=True, targets='all', block=False):
        """Shutdown the executor, including all RADICAL-Pilot
           components and redis instances
        """

        super().shutdown(hub=True, targets='all', block=False)
        self.redis.flushall()
        return True


# ------------------------------------------------------------------------------

