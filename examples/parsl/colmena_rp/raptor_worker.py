import os
import sys
import time
import redis
import threading     as mt
import radical.utils as ru

import radical.pilot as rp
import radical.pilot.utils as rpu


_Resources    = rp.raptor.worker_mpi._Resources
_TaskPuller   = rp.raptor.worker_mpi._TaskPuller
_ResultPusher = rp.raptor.worker_mpi._ResultPusher

# before you start, put sim.py in $HOME
home = os.getenv("HOME")
sys.path.append(home)

class Worker(rp.raptor.worker_mpi._Worker):

    def __init__(self, rank_task_q_get, rank_result_q_put,
                 event, log, prof, base):

        super().__init__(rank_task_q_get, rank_result_q_put,
                         event, log, prof, base)    

        cfg = ru.read_json('raptor.cfg')
        self._host = cfg.get('redis').get('host', '127.0.0.1')
        self._port = cfg.get('redis').get('port', 6379)
        self._pass = cfg.get('redis').get('pass', None)


    def run(self):

        from mpi4py import MPI

        self._world = MPI.COMM_WORLD
        self._group = self._world.Get_group()
        self._rank  = self._world.rank
        self._ranks = self._world.size

        try:

            self._log.debug('=== init redis [%s] [%d]', self._host, self._port)
            if self._host and self._port:
                self.redis  = redis.Redis(host=self._host, port=self._port,
                                                       password=self._pass)
    
            self._log.debug('=== init worker [%d] [%d] rtq_get:%s rrq_put:%s',
                            self._rank, self._ranks,
                            self._rank_task_q_get, self._rank_result_q_put)

            # get tasks from rank 0
            rank_task_q = ru.zmq.Getter('rank_tasks', url=self._rank_task_q_get,
                                        log=self._log, prof=self._prof)
            # send results back to rank 0
            rank_result_q = ru.zmq.Putter('rank_results',
                                          url=self._rank_result_q_put,
                                          log=self._log,  prof=self._prof)

            # signal success
            self._event.set()

            # get tasks, do them, push results back
            while True:

                tasks = rank_task_q.get_nowait(qname=str(self._rank), timeout=100)

                if not tasks:
                    continue

                assert(len(tasks) == 1)
                task = tasks[0]
                if task['name'] == 'colmena':
                    # make sure we are conneced
                    assert(self.redis.ping())
                    # pull from redis queue if we have a colmena task
                    key = 'task:{0}'.format(task['uid'])
                    redis_task = eval(self.redis.get(key))
                    if redis_task:
                        self._log.debug('redis_task====>')
                        self._log.debug((redis_task))
                        task['description']['function'] = redis_task['function']

                self._log.debug('==== %s 2 - task recv by %d', task['uid'], self._rank)

                # FIXME: how can that be?
                if self._rank not in task['ranks']:
                    raise RuntimeError('internal error: inconsistent rank info')

                comm  = None
                group = None

                # FIXME: task_exec_start
                try:
                    out, err, ret, val, exc = self._dispatch(task)
                    self._log.debug('dispatch result: %s: %s', task['uid'], out)

                    task['error']        = None
                    task['stdout']       = out
                    task['stderr']       = err
                    task['exit_code']    = ret
                    task['return_value'] = val
                    task['exception']    = exc

                except Exception as e:
                    import pprint
                    self._log.exception('work failed: \n%s',
                                        pprint.pformat(task))
                    task['error']        = repr(e)
                    task['stdout']       = ''
                    task['stderr']       = str(e)
                    task['exit_code']    = -1
                    task['return_value'] = None
                    task['exception']    = [e.__class__.__name__, str(e)]
                    self._log.exception('recv err  %s  to  0' % (task['uid']))

                finally:
                    # sub-communicator must always be destroyed
                    if group: group.Free()
                    if comm : comm.Free()

                    # send task back to rank 0
                    self._log.info('===> %s from %d to %d',
                                   task['uid'], self._rank, 0)

                    # FIXME: task_exec_stop
                    self._log.debug('==== put 0 %s : %s', task['uid'], os.getpid())

                    if task['name'] == 'colmena':
                        assert(self.redis.ping())
                        self._log.debug('redis_result_task_%s====>', task['uid'])
                        self._log.debug(str(task['return_value']))

                        key = 'result:{0}'.format(str(task['uid']))
                        ret = rpu.serialize_obj(task['return_value'])

                        task['stdout']                  = 'redis'
                        task['return_value']            = str(ret)
                        task['description']['function'] = 'redis'
                        self.redis.set(key, str(task))

                    rank_result_q.put(task)

        except:
            self._log.exception('work thread failed [%s]', self._rank)



class WorkerRedis(rp.raptor.MPIWorker):

    def __init__(self, cfg):
        super().__init__(cfg)

    def start(self):
        # all ranks run a worker thread
        # the worker should be started before the managers as the manager
        # contacts the workers with queue endpoint information
        self._log.info('=== rank %s starts [%s]', self._rank, self._manager)
        worker_ok = mt.Event()
        self._work_thread = Worker(rank_task_q_get   = self._rank_task_q_get,
                                   rank_result_q_put = self._rank_result_q_put,
                                   event             = worker_ok,
                                   log               = self._log,
                                   prof              = self._prof,
                                   base              = self)
        self._work_thread.start()

        worker_ok.wait(timeout=60)
        self._log.info('=== rank %s starts worker [%s]', self._rank, self._manager)

        if not worker_ok.is_set():
            raise RuntimeError('failed to start worker thread')


        # the manager (rank 0) will start two threads - one to pull tasks from
        # the master, one to push results back to the master
        if self._manager:

            self._log.info('=== rank %s starts managers', self._rank)

            resources = _Resources(self._log,   self._prof, self._ranks)
            self._log.info('=== resources: %s', resources)

            # rank 0 spawns manager threads
            pull_ok = mt.Event()
            push_ok = mt.Event()

            self._log.debug('=== wrq_put:%s wtq_get:%s',
                            self._worker_result_q_put, self._worker_task_q_get)

            self._pull = _TaskPuller(
                    worker_task_q_get   = self._worker_task_q_get,
                    worker_result_q_put = self._worker_result_q_put,
                    rank_task_q_put     = self._rank_task_q.addr_put,
                    event               = pull_ok,
                    resources           = resources,
                    log                 = self._log,
                    prof                = self._prof)

            self._push = _ResultPusher(
                    worker_result_q_put = self._worker_result_q_put,
                    rank_result_q_get   = self._rank_result_q.addr_get,
                    event               = push_ok,
                    resources           = resources,
                    log                 = self._log,
                    prof                = self._prof)

            self._pull.start()
            self._push.start()

            self._log.debug('=== start wait')

            pull_ok.wait(timeout=60)
            self._log.debug('=== wait pull ok')
            push_ok.wait(timeout=60)
            self._log.debug('=== wait push ok')

            if not pull_ok.is_set():
                raise RuntimeError('failed to start pull thread')

            if not push_ok.is_set():
                raise RuntimeError('failed to start push thread')

        self._log.info('=== rank %s starts [%s] ok', self._rank, self._manager)

