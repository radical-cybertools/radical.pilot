#!/usr/bin/env python3


import msgpack

# pylint: disable=import-error
from mpi4py import MPI                                                    # noqa


IDLE = False
BUSY = True


# ------------------------------------------------------------------------------
#
class MPIExecutor(object):
    '''
    This class executes MPI functions in a new, private sub-communicator.

    The class should be used as follows:

        executor = MPIExecutor()
        executor.work()

    The code above should run in *all* ranks of `MPI_COMM_WORLD`, i.e., the code
    above should run under (for example):

        mpirun -n 16 --use-hwthread-cpus executor.py

    The class will run `self.work_master()` on rank 0, and `self.work_worker()`
    on all other ranks.

    Master:
    -------

    The master then iterates over incoming tasks (well, over a static task list
    at the moment) and will try to find as many idle workers as the task
    requires (`task['ranks']`).  It then sends a direct MPI message (tag `0`) to
    exactly those workers.

    Once all tasks are sent, the master will collect all task results (`recv`
    with message tag `1`).  Once all results are received, the master will send
    a termination message to all workers (tag `0`, message `None`), and will
    terminate -- as will the workers when receiving that message.


    Worker:
    -------

    The worker will wait for an incoming message with tag `0` (task to run).
    From the received task it will determine with what other workers it should
    create a  sub-communicator.  The respective process group is created as is
    the new communicator, and the workload (the methods `self.test_1` or
    `self.test_2`) are called, passing the sub-communicator as first argument.
    The methods can then use the communicator to communicate, and will
    eventually return.  Whatever value rank `0` of that sub-communicator will
    return is stored as result of the task, and the worker of that rank `0` will
    send the task back to the master (tag `1`).

    If the worker receives a `None` message, it will terminate.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        # ensure we have a communicator
        self._world = MPI.COMM_WORLD
        self._group = self._world.Get_group()


    # --------------------------------------------------------------------------
    #
    @property
    def rank(self) -> int:
        return self._world.rank

    @property
    def size(self) -> int:
        return self._world.size

    @property
    def master(self) -> bool:
        return bool(self.rank == 0)

    @property
    def worker(self) -> bool:
        return bool(self.rank != 0)


    # --------------------------------------------------------------------------
    #
    def test_1(self, comm, data):
        '''
        test workload - collect rank IDs of sub-communicator
        '''

        if comm.rank == 0:
            # collect data from all other ranks
            result = [data, 0]
            for _ in range(comm.size - 1):
                result.append(comm.recv())
            return result
        else:
            # send data to rank 0
            comm.send(comm.rank, dest=0)


    # --------------------------------------------------------------------------
    #
    def test_2(self, comm, data):
        '''
        test workload - collect rank IDs of sub-communicator as strings
        '''

        if comm.rank == 0:
            # collect data from all other ranks
            result = [data, 0]
            for _ in range(comm.size - 1):
                result.append(str(comm.recv()))
            return result
        else:
            # send data to rank 0
            comm.send(comm.rank, dest=0)


    # --------------------------------------------------------------------------
    #
    def _out(self, msg):
        '''
        small helper for debug output
        '''

        if self.master: print('=== %3d: %s' % (self.rank, msg))
        else          : print('  - %3d: %s' % (self.rank, msg))


    # --------------------------------------------------------------------------
    #
    def work(self):
        '''
        master (rank == 0) does master stuff
        worker (rank != 0) does worker stuff
        '''

        if self.master: self.work_master()
        else          : self.work_worker()


    # --------------------------------------------------------------------------
    #
    def work_master(self):

        # keep track of busy and idle workers (not really used, yet)
        resources = [IDLE] * (self.size - 1)

        # define a static list of tasks
        #    uid  : duh!
        #    ranks: how many workers to use in the task's communicator
        #    work : what method to call
        #    args : what arguments to pass to the method
        tasks = [{'uid': 0, 'ranks': 2, 'work' : 'test_1', 'args': ['foo']},
                 {'uid': 1, 'ranks': 4, 'work' : 'test_2', 'args': ['bar']},
                 {'uid': 2, 'ranks': 4, 'work' : 'test_1', 'args': ['buz']},
                 {'uid': 3, 'ranks': 3, 'work' : 'test_2', 'args': ['biz']},
                ]

        for task in tasks:

            # find workers and mark as busy
            workers = list()
            for idx, worker in enumerate(resources):
                if worker == IDLE:
                    workers.append(idx + 1)
                    resources[idx] = BUSY
                    if len(workers) == task['ranks']:
                        # enough workers found
                        break

            # make sure we did find workers!
            assert(len(workers) == task['ranks'])

            # workers need to know who is part of the sub-communicator
            task['workers'] = workers

            # send the task to each of the workers
            self._out('send task %s  to  %s' % (task['uid'], workers))
            for worker in workers:
                self._world.send(msgpack.packb(task), dest=worker, tag=0)


        # once all tasks are sent out, collect results
        for task in tasks:
            task = msgpack.unpackb(self._world.recv(tag=1))
            self._out('recv task %s from %s' % (task['uid'], task))


        # once all results are received, send termination signal to workers
        for worker in range(self.size - 1):
            self._out('send term to %s' % (worker + 1))
            self._world.send(msgpack.packb(None), dest=worker + 1, tag=0)


    # --------------------------------------------------------------------------
    #
    def work_worker(self):

        # wait for termination message
        while True:

            task = msgpack.unpackb(self._world.recv(source=0, tag=0))

            if not task:
                self._out('terminate!')
                break

            self._out('recv task %s from 0 >>>' % (task['uid']))
            assert(self.rank in task['workers'])

            comm  = None
            group = None

            try:
                # create new communicator with all workers assigned to this task
                group = self._group.Incl(task['workers'])
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
                    self._world.send(msgpack.packb(task), dest=0, tag=1)

            except Exception as e:
                task['error'] = str(e)
                self._out('recv err  %s  to  0' % (task['uid']))
                self._world.send(msgpack.packb(task), dest=0, tag=1)
                raise

            finally:
                # sub-communicator must alwaus be destroyed
                if group: group.Free()
                if comm : comm.Free()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    executor = MPIExecutor()
    executor.work()


# ------------------------------------------------------------------------------

