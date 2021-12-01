
import os
import time

import threading         as mt
import multiprocessing   as mp

import radical.utils     as ru

from .worker import Worker


# MPI message tags
TAG_REGISTER_REQUESTS    = 10
TAG_REGISTER_REQUESTS_OK = 11
TAG_REGISTER_RESULTS     = 20
TAG_REGISTER_RESULTS_OK  = 21

# message payload constants
MSG_OK  = 10
MSG_NOK = 20

# resource allocation flags
FREE = 0
BUSY = 1


# ------------------------------------------------------------------------------
#
class MPIWorker(Worker):
    '''
    This worker manages a certain number of cores and gpus.  The master will
    start this worker by placing one rank per managed core (the GPUs are used
    dynamically).

    The first rank (rank 0) will manage the worker and for that purpose spawns
    two threads.  The first will pull tasks from the master's queue, and upon
    arrival will:

      - schedule incoming tasks over the available ranks
      - sent each target rank the required task startup info

    The second thread will collect the results from the tasks and send them back
    to the master.

    The main thread of rank 0 will function like the other threads: wait for
    task startup info and enact them.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, session=None):

        from mpi4py import MPI

        self._world = MPI.COMM_WORLD
        self._group = self._world.Get_group()

        self._rank  = int(os.environ.get('RP_RANK',  -1))
        self._ranks = int(os.environ.get('RP_RANKS', -1))

        if self._rank < 0:
            raise RuntimeError('MPI worker needs MPI')

        if self._ranks < 1:
            raise RuntimeError('MPI worker needs more than one rank')

        if self._rank == 0: self._manager = True
        else              : self._manager = False

        # rank 0 will register the worker with the master and connect
        # to the task and result queues
        super().__init__(cfg=cfg, session=session, register=self._manager)

        # FIXME: RP considers MPI tasks to be homogeneous, in that all ranks of
        #        the task have the same set of resources allocated.  That
        #        implies that all ranks in this MPI worker have the same
        #        resources allocated.  That implies that, in most cases, no rank
        #        has GPUs alloated (we place one rank per core, but the number
        #        of cores per node is in general different than the number of
        #        GPUs per node).
        #
        #        RP will need to support heterogeneous MPI tasks to allow this
        #        worker to also assign GPUs to specific ranks.
        #
        self._res_evt   = mp.Event()  # signals free resources
        self._res_lock  = mp.Lock()   # lock resource for alloc / deallock
        self._resources = {
                'cores': [0] * self._ranks
              # 'gpus' : [0] * self._n_gpus
        }


        # all ranks run a worker thread
        self._worker = mt.Thread(target=self._work)
        self._worker.daemon = True
        self._worker.start()

        # the manager (rank 0) will start two threads - one to pull tasks from
        # the master, one to push results back to the master
        if self._manager:
            self._start_manager_threads()


    # --------------------------------------------------------------------------
    #
    def _start_manager_threads(self):

        # rank 0 spawns manager threads
        pull_ok = mt.Event()
        push_ok = mt.Event()

        pull_thread = mt.Thread(target=self._pull_from_master, args=[pull_ok])
        push_thread = mt.Thread(target=self._push_to_master,   args=[push_ok])

        pull_thread.daemon = True
        push_thread.daemon = True

        pull_thread.start()
        push_thread.start()

        pull_ok.wait(timeout=1)
        push_ok.wait(timeout=1)

        if not pull_ok.is_set():
            raise RuntimeError('failed to start pull thread')

        if not push_ok.is_set():
            raise RuntimeError('failed to start push thread')


    # --------------------------------------------------------------------------
    #
    def _pull_from_master(self, event):
        '''
        This thread pulls tasks from the master, schedules resources for the
        tasks, and pushes them out to the respective ranks for execution.  If
        a task arrives for which no resources are available, the thread will
        block until such resources do become available.
        '''

        try:

            # resources are initially all free
            self._res_evt.set()

            # connect to the master's task queue
            getter = ru.zmq.Getter('request', self._info.req_addr_get)

            # for each worker, create one push pipe endpoint.  The worker will
            # pull work from that pipe once work gets assigned to it
            putters = dict()
            for rank in range(self._ranks):
                pipe = ru.zmq.Pipe()
                pipe.connect_push()
                putters[rank] = pipe

            # inform the ranks about their pipe endpoint
            for rank in range(self._ranks):
                data = {'pipe_requests': putters[rank].url}
                self._world.send(data, dest=rank, tag=TAG_REGISTER_REQUESTS)

            self._collect_replies(tag=TAG_REGISTER_REQUESTS_OK, expected=MSG_OK)

            # setup is completed - signal main thread
            event.set()

            # pull tasks, allocate ranks to it, send task to those ranks
            while not self._term.is_set():

                tasks = getter.get_nowait(timeout=0.1)

                if not tasks:
                    continue

                self._log.debug('tasks: %s', len(tasks))

                for task in tasks:

                    cores = task.get('cores', 1)

                    while True:

                        ranks = self._alloc(cores)
                        if ranks:
                            task['ranks'] = ranks
                            break

                        # no free ranks found, wait for free resources and try again
                        self._res_evt.wait(timeout=1)

                        # don't block termination
                        if self._term.is_set():
                            return

                    assert(task['ranks'])

                    # found free ranks - send task
                    for idx,rank in enumerate(task['ranks']):
                        putters[rank].put(task)

        except:
            self._log.exception('pull thread failed [%s]', self._rank)



    # --------------------------------------------------------------------------
    #
    def _push_to_master(self, event):

        try:
            # connect back to the master
            putter = ru.zmq.Putter('result', self._info.res_addr_put)

            # create a result pipe for the workers to report results back
            getter = ru.zmq.Pipe()
            getter.connect_pull()

            # inform the ranks about the pipe endpoint
            for rank in range(self._ranks):
                data = {'pipe_results': getter.url}
                self._world.send(data, dest=rank, tag=TAG_REGISTER_RESULTS)

            # wait for workers to ping back
            self._collect_replies(tag=TAG_REGISTER_RESULTS_OK, expected=MSG_OK)

            # setup is completed - signal main thread
            event.set()


            # pull results from ranks and push them to the master
            while not self._term.is_set():

                task = getter.get_nowait(timeout=0.1)

                if not task:
                    continue

                self._dealloc(task['ranks'])

                putter.put(task)

        except:
            self._log.exception('push thread failed [%s]', self._rank)


    # --------------------------------------------------------------------------
    #
    def _collect_replies(self, tag, expected, timeout=10):

        # wait for workers to ping back
        start = time.time()
        ok    = 0

        while ok < self._ranks:

            if time.time() - start > timeout:
                break

            check = self._world.Iprobe(tag=tag)
            if not check:
                time.sleep(0.1)
                continue

            msg = self._world.recv(tag=tag)

            if msg != expected:
                raise  RuntimeError('worker rank failed: %s' % msg)
            ok += 1

        if ok != self._ranks:
            raise RuntimeError('could not collect from all workers')


    # --------------------------------------------------------------------------
    #
    def _work(self):

        try:
            # wait for a first initialization message which will provide us
            # with the addresses of the pipe to pull tasks from and the pipe
            # to push results to.
            req_info = self._world.recv(source=0, tag=TAG_REGISTER_REQUESTS)
            res_info = self._world.recv(source=0, tag=TAG_REGISTER_RESULTS)

            assert('pipe_requests' in req_info), req_info
            assert('pipe_results'  in res_info), res_info

            self._world.send(MSG_OK, dest=0, tag=TAG_REGISTER_REQUESTS_OK)
            self._world.send(MSG_OK, dest=0, tag=TAG_REGISTER_RESULTS_OK)

            # get tasks, do them, push results back (if rank 0)
            getter = ru.zmq.Pipe()
            getter.connect_pull(req_info['pipe_requests'])

            putter = ru.zmq.Pipe()
            putter.connect_push(res_info['pipe_results'])

            while not self._term.is_set():

                # fetch task, join communicator, run task
                task = getter.get_nowait(timeout=0.1)

                if not task:
                    continue

                self._log.debug('recv task %s', task['uid'])
                assert(self._rank in task['ranks'])

                comm  = None
                group = None

                try:
                    # create new communicator with all workers assigned to this task
                    group = self._group.Incl(task['ranks'])
                    comm  = self._world.Create_group(group)
                    assert(comm)

                    # work on task
                    to_call = getattr(self, task['work'], None)
                    assert(to_call)
                    result = to_call(comm, *task['args'])

                    # result is only reported back by rank 0 (of sub-communicator)
                    if comm.rank == 0:
                        task['result'] = result
                        self._out('send res  %s  to  0 <<< ' % (task['uid']))
                        putter.put(task)

                except Exception as e:
                    import pprint
                    self._log.exception('work failed: \n%s',
                            pprint.pformat(task))
                    task['error'] = str(e)
                    self._out('recv err  %s  to  0' % (task['uid']))
                    self._world.send(task, dest=0, tag=1)

                finally:
                    # sub-communicator must alwaus be destroyed
                    if group: group.Free()
                    if comm : comm.Free()

        except:
            self._log.exception('work thread failed [%s]', self._rank)


    # --------------------------------------------------------------------------
    #
    def _alloc(self, cores):
        '''
        allocate task cores for ranks
        '''

        # FIXME: no GPU support, yet
        #
        with self._res_lock:


            assert(cores >= 1)
            assert(cores <= self._ranks)

            if cores > self._resources['cores'].count(FREE): return False

            ranks = list()
            for rank in range(self._ranks):
                if self._resources['cores'][rank] == FREE:
                    self._resources['cores'][rank] = BUSY
                    ranks.append(rank)
                    if len(ranks) == cores:
                        break

            return ranks


    # --------------------------------------------------------------------------
    #
    def _dealloc(self, ranks):
        '''
        deallocate task ranks
        '''

        with self._res_lock:

            for rank in ranks:
                assert(self._resources['cores'][rank])
                self._resources['cores'][rank] = FREE

            # signal available resources
            self._res_evt.set()

            return True


# ------------------------------------------------------------------------------

