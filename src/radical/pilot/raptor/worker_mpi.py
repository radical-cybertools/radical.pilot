import os
import dill
import time
import queue
import codecs
import pickle
import msgpack

# FIXME: I beleive RP hangs here
# removing this line will allow RP
# to continue executing until failing
from mpi4py import MPI

import threading         as mt
import multiprocessing   as mp

import radical.utils     as ru

from .. import constants as rpc

from .worker import Worker

IDLE = False
BUSY = True

MPI.pickle.__init__(dill.dumps, dill.loads)

# -----------------------------------------------------------------------------
#


class MPIWorker(Worker):

    """
    MPIWorker diagram
    ---------------------------------------------------------------------------
        Raptor Master     |                      Raptor MPIWorker           
    ----------------------|----------------------------------------------------                                                    
                          |                           |
    (1)  register OK!   <--->     [registered]        |   
                          |                           |  
                          |                       (MPI_send)
                          |                           |
                                                      v      
    (2)  send MPI_func  <--->  request_cb[recv task] (3) --> | mpi_worker
                          |                                  | (execute)
                          |                                  |     |
                          |                                  |     v
                          |                                  | queue.put(res)
    ---------------------------------------------------------------------------
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, session=None):
        rank = None

        if rank is None: rank = os.environ.get('PMIX_RANK')
        if rank is None: rank = os.environ.get('PMI_RANK')
        if rank is None: rank = os.environ.get('OMPI_COMM_WORLD_RANK')

        if rank is None: rank = 0
        else           : rank = int(rank)

        # only rank 0 registers with the master
        if rank == 0: register = True
        else        : register = False

        super().__init__(cfg=cfg, session=session, register=register)

        # keep worker ID and rank
        self._cfg['rank'] = rank
        self._cfg['uid']  = '%s.%03d' % (self._cfg['uid'], rank)

        self._n_cores = self._cfg.worker_descr.cores_per_rank
        self._n_gpus  = self._cfg.worker_descr.gpus_per_rank

        self._res_evt = mp.Event()          # set on free resources
        self._mlock   = ru.Lock(self._uid)  # lock `_modes`
        self._modes   = dict()              # call modes (call, exec, eval, ...)

        # ensure we have a communicator
        self._world      = MPI.COMM_WORLD
        self._group      = self._world.Get_group()
        self._pwd        = os.getcwd()
        self._log        = ru.Logger(self._uid,   ns='radical.pilot', path=self._pwd)
        self._prof       = ru.Profiler(self._uid, ns='radical.pilot', path=self._pwd)

        self._resources = {'cores' : [0] * self._n_cores,
                           'gpus'  : [0] * self._n_gpus}

        if self._cfg['rank'] == 0 :
            self._res_evt.set()
            self._result_queue  = mp.Queue()
            self._result_thread = mt.Thread(target=self._result_watcher)
            self._result_thread.daemon = True
            self._result_thread.start()

            # keep track of busy and idle workers
            self.resources = [IDLE] * (self._world.size - 1)

        self.register_mode('mpi_worker',  self.mpi_worker)
        # prepare base env dict used for all tasks
        self._task_env = dict()
        for k,v in os.environ.items():
            if k.startswith('RP_'):
                self._task_env[k] = v

    # --------------------------------------------------------------------------
    #
    @property
    def rank(self) -> int:
        return self._world.rank


    # --------------------------------------------------------------------------
    #
    def pre_exec(self):
        '''
        This method can be overloaded by the Worker implementation to run any
        pre_exec commands before spawning worker processes.
        '''
        pass


    # --------------------------------------------------------------------------
    #
    def task_post_exec(self, task):
        '''
        This method is called upon completing a request, and can be
        overloaded to perform any cleanup action before the request is reported
        as complete.
        '''
        pass


    # --------------------------------------------------------------------------
    #
    def register_mode(self, name, executor):

        assert(name not in self._modes)

        self._modes[name] = executor


    # --------------------------------------------------------------------------
    #
    def request_cb(self, tasks):

        # define a static list of tasks and send single one per time to the workers
        # we assume in this mode that the task is large enough to occupy the 
        # resources:
        # task[ranks] == self.resources
        # uid  : recieved task id
        # ranks: how many workers to use in the task's communicator
        # work : what method to call
        # args : what arguments to pass to the method

        # resources are initially all free
        if self.rank == 0:

            for task in ru.as_list(tasks):

                task['worker'] = self._uid

                try:
                    self._log.debug('got %s', task['uid'])
                    task_descr = task['description']
                    task_exe   = task_descr['executable']
                    task_ranks = task_descr['cpu_processes']
                    # ok, we have work to do.  Check the requirements to see how
                    # many cpus and gpus we need to mark as busy

                    try:
                        task_info = pickle.loads(codecs.decode(task_exe.encode(),"base64"))
                        task['work']  = task_info["_cud_code"]
                        task['args']  = task_info["_cud_args"]
                        task['ranks'] = task_ranks

                    except Exception as e:
                        self._log.debug('ERROR %s', str(e))

                    while not self._alloc_task(task):
                        # no resource - wait for new resources
                        #
                        # NOTE: this will block smaller tasks from being executed
                        #       right now.  alloc_task is not a proper scheduler,
                        #       after all.
                        time.sleep(0.01)

                        # break on termination
                        if self._term.is_set():
                            return False

                        self._res_evt.clear()

                    self._prof.prof('req_start', uid=task['uid'], msg=self._uid)

                    env = self._task_env
                    env['RP_TASK_ID'] = task['uid']

                    # find workers internally and mark as busy
                    workers = list()
                    for idx, worker in enumerate(self.resources):
                        if worker == IDLE:
                            workers.append(idx + 1)
                            self.resources[idx] = BUSY
                            if len(workers) == task_ranks:
                                self._log.debug('enough workers found')
                                break

                    # make sure we did find workers!
                    assert(len(workers) == task_ranks)

                    # workers need to know who is part of the sub-communicator
                    task['workers'] = workers

                    # send the task to each of the workers
                    self._log.debug('send task %s  to  %s' % (task['uid'], workers))
                    for worker in workers:
                        self._world.send(msgpack.packb(task), dest=worker, tag=0)


                except Exception as e:
                    self._log.exception('request failed')

                    # free the internal workers for failed task
                    idle_workers = []
                    for idx, worker in enumerate(self.resources):
                        if worker == BUSY:
                            self.resources[idx] = IDLE
                            self._log.debug('marked worker number %d as IDLE' % (idx))
                            idle_workers.append(idx)
                            if  len(idle_workers) == task['ranks']:
                                break

                    # free resources again for failed task
                    self._dealloc_task(task)
                    self._result_queue.put(str(e))


    # --------------------------------------------------------------------------
    #
    def mpi_worker(self):

        # wait for termination message
        while True:

            task = msgpack.unpackb(self._world.recv(source=0, tag=0))

            if not task:
                self._log.debug('terminate!')
                break

            self._log.debug('recv task %s from Master >>>' % (task['uid']))
            assert(self.rank in task['workers'])

            comm  = None
            group = None

            try:
                # create new communicator with all workers assigned to this task
                group = self._group.Incl(task['workers'])
                comm  = self._world.Create_group(group)
                assert(comm)

                # work on task
                fn = MPI.pickle.loads(task['work'])
                result = fn(*task['args'])

                # result is only reported back by rank 0 (of sub-communicator)
                if comm.rank == 0:
                    task['result'] = result
                    task['state']  = "DONE"
                    self._log.debug('send %s result to master <<< ' % (task['uid']))
                    self._result_queue.put(task['result'])

            except Exception as e:
                if comm.rank == 0:
                    task['error']  = str(e)
                    task['state']  = "FAILED"
                    task['result'] = None

                    self._log.error('%s %s (%s)' % (task['uid'], task['state'],
                                                task['error']))
                    self._result_queue.put(task['error'])
                raise

            finally:
                # sub-communicator must always be destroyed
                if group: group.Free()
                if comm : comm.Free()

                # once the task executing is finished mark the internal workers as IDLE
                if comm.rank == 0:
                    idle_workers = []
                    for idx, worker in enumerate(self.resources):
                        if worker == BUSY:
                            self.resources[idx] = IDLE
                            self._log.debug('marked worker number %d as IDLE' % (idx))
                            idle_workers.append(idx)
                            if  len(idle_workers) == task['ranks']:
                                break


    # --------------------------------------------------------------------------
    #
    def _alloc_task(self, task):
        '''
        allocate task resources
        '''

        with self._mlock:

            cores = task.get('cores', 1)
            gpus  = task.get('gpus' , 0)

            assert(cores >= 1)
            assert(cores <= self._n_cores)
            assert(gpus  <= self._n_gpus)

            if cores > self._resources['cores'].count(0): return False
            if gpus  > self._resources['gpus' ].count(0): return False

            alloc_cores = list()
            alloc_gpus  = list()

            if cores:
                for n in range(self._n_cores):
                    if not self._resources['cores'][n]:
                        self._resources['cores'][n] = 1
                        alloc_cores.append(n)
                        if len(alloc_cores) == cores:
                            break

            if gpus:
                for n in range(self._n_gpus):
                    if not self._resources['gpus'][n]:
                        self._resources['gpus'][n] = 1
                        alloc_gpus.append(n)
                        if len(alloc_gpus) == gpus:
                            break

            task['resources'] = {'cores': alloc_cores,
                                 'gpus' : alloc_gpus}
            return True


    # --------------------------------------------------------------------------
    #
    def _dealloc_task(self, task):
        '''
        deallocate task resources
        '''

        with self._mlock:

            resources = task['resources']

            for n in resources['cores']:
                assert(self._resources['cores'][n])
                self._resources['cores'][n] = 0

            for n in resources['gpus']:
                assert(self._resources['gpus'][n])
                self._resources['gpus'][n] = 0

            # signal available resources
            self._res_evt.set()

            return True


    # --------------------------------------------------------------------------
    #
    def _result_watcher(self):

        try:
            while not self._term.is_set():

                try:
                    res = self._result_queue.get(timeout=0.1)
                    self._log.debug('got   result: %s', res)
                    self._result_cb(res)

                except queue.Empty:
                    pass

        except:
            self._log.exception('queue error')
            raise

        finally:
            # FIXME: we should unregister for all ranks on error maybe?
            if self._cfg['rank'] == 0:
                self.publish(rpc.CONTROL_PUBSUB,
                             {'cmd': 'worker_unregister',
                              'arg': {'uid' : self._cfg['uid']}})


    # --------------------------------------------------------------------------
    #
    def _result_cb(self, result):

        try:
            task, out, err, ret, val = result
            self._log.debug('result cb: task %s', task['uid'])

            # free resources again for the task
            self._dealloc_task(task)

            res = {'req': task['uid'],
                   'out': out,
                   'err': err,
                   'ret': ret,
                   'val': val}

            self._res_put.put(res)
            self.task_post_exec(task)
            self._prof.prof('req_stop', uid=task['uid'], msg=self._uid)

        except:
            self._log.exception('result cb failed')
            raise


    # --------------------------------------------------------------------------
    #
    def _error_cb(self, error):

        self._log.debug('error: %s', error)
        raise RuntimeError(error)
