#!/usr/bin/env python3

import os
import sys
import time

import radical.utils as ru
import radical.pilot as rp

from radical.pilot import PythonTask


# This script has to run as a task within an pilot allocation, and is
# a demonstration of a task overlay within the RCT framework.
# It will:
#
#   - create a master which bootstraps a specific communication layer
#   - insert n workers into the pilot (again as a task)
#   - perform RPC handshake with those workers
#   - send RPC requests to the workers
#   - terminate the worker
#
# The worker itself is an external program which is not covered in this code.

pytask = PythonTask.pythontask


@pytask
def func_mpi(comm, msg, sleep=0):
    # pylint: disable=reimported
    import time
    print('hello %d/%d: %s' % (comm.rank, comm.size, msg))
    time.sleep(sleep)



@pytask
def func_non_mpi(a):
    # pylint: disable=reimported
    import math
    import random
    b = random.random()
    t = math.exp(a * b)
    return t


# ------------------------------------------------------------------------------
#
class MyMaster(rp.raptor.Master):
    '''
    This class provides the communication setup for the task overlay: it will
    set up the request / response communication queues and provide the endpoint
    information to be forwarded to the workers.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        self._cnt = 0
        self._submitted = {rp.TASK_EXECUTABLE  : 0,
                           rp.TASK_FUNCTION    : 0,
                           rp.TASK_EVAL        : 0,
                           rp.TASK_EXEC        : 0,
                           rp.TASK_PROC        : 0,
                           rp.TASK_SHELL       : 0}
        self._collected = {rp.TASK_EXECUTABLE  : 0,
                           rp.TASK_FUNCTION    : 0,
                           rp.TASK_EVAL        : 0,
                           rp.TASK_EXEC        : 0,
                           rp.TASK_PROC        : 0,
                           rp.TASK_SHELL       : 0}

        # initialize the task overlay base class.  That base class will ensure
        # proper communication channels to the pilot agent.
        rp.raptor.Master.__init__(self, cfg=cfg)


    # --------------------------------------------------------------------------
    #
    def submit(self):

        self._prof.prof('create_start')

        # create additional tasks to be distributed to the workers.

        tds = list()
        for i in range(1):

            tds.append(rp.TaskDescription({
                'uid'             : 'task.exe.m.%06d' % i,
                'mode'            : rp.TASK_EXECUTABLE,
                'scheduler'       : None,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'executable'      : '/bin/sh',
                'arguments'       : ['-c',
                                     'echo "hello $RP_RANK/$RP_RANKS: $RP_TASK_ID"']}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.call.m.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNCTION,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'function'        : 'hello_mpi',
                'kwargs'          : {'msg': 'task.call.m.%06d' % i},
                'scheduler'       : 'master.000000'}))

            bson = func_mpi(None, msg='task.call.m.%06d' % i, sleep=0)
            tds.append(rp.TaskDescription({
                'uid'             : 'task.mpi_ser_func.m.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNCTION,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'function'        : bson,
                'scheduler'       : 'master.000000'}))
            self._log.info('bson %s : %s : %s' % (tds[-1]['uid'], len(bson), bson))

            bson = func_non_mpi(i)
            tds.append(rp.TaskDescription({
                'uid'             : 'task.ser_func.m.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNCTION,
                'cpu_processes'   : 2,
                'function'        : bson,
                'scheduler'       : 'master.000000'}))
            self._log.info('bson %s : %s : %s' % (tds[-1]['uid'], len(bson), bson))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.eval.m.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_EVAL,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'code'            :
                    'print("hello %s/%s: %s" % (os.environ["RP_RANK"],'
                    'os.environ["RP_RANKS"], os.environ["RP_TASK_ID"]))',
                'scheduler'       : 'master.000000'}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.exec.m.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_EXEC,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'code'            :
                    'import os\nprint("hello %s/%s: %s" % (os.environ["RP_RANK"],'
                    'os.environ["RP_RANKS"], os.environ["RP_TASK_ID"]))',
                'scheduler'       : 'master.000000'}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.proc.m.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_PROC,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'executable'      : '/bin/sh',
                'arguments'       : ['-c', 'echo "hello $RP_RANK/$RP_RANKS: '
                                           '$RP_TASK_ID"'],
                'scheduler'       : 'master.000000'}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.shell.m.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_SHELL,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'command'         : 'echo "hello $RP_RANK/$RP_RANKS: $RP_TASK_ID"',
                'scheduler'       : 'master.000000'}))


        self.submit_tasks(tds)

        self._prof.prof('create_stop')

        # wait for outstanding tasks to complete
        while True:

            completed = sum(self._collected.values())
            submitted = sum(self._submitted.values())

            if submitted:
                # request_cb has been called, so we can wait for completion

                self._log.info('exec done?: %d >= %d ', completed, submitted)

                if completed >= submitted:
                  # self.stop()
                    break

            time.sleep(1)

        self._log.info('exec done!')


    # --------------------------------------------------------------------------
    #
    def request_cb(self, tasks):

        for task in tasks:

            self._log.debug('request_cb %s\n' % (task['uid']))

            mode = task['description']['mode']
            # uid  = task['description']['uid']

            self._submitted[mode] += 1

          # # for each `function` mode task, submit one more `proc` mode request
          # if mode == rp.TASK_FUNCTION:
          #     self.submit_tasks(rp.TaskDescription(
          #         {'uid'             : uid.replace('call', 'extra'),
          #        # 'timeout'         : 10,
          #          'mode'            : rp.TASK_PROC,
          #          'cpu_processes'   : 2,
          #          'cpu_process_type': rp.MPI,
          #          'executable'      : '/bin/sh',
          #          'arguments'       : ['-c', 'echo "hello $RP_RANK/$RP_RANKS: '
          #                                     '$RP_TASK_ID"'],
          #          'scheduler'       : 'master.000000'}))

        return tasks


    # --------------------------------------------------------------------------
    #
    def result_cb(self, tasks):

        for task in tasks:

            mode = task['description']['mode']
            self._collected[mode] += 1

            # NOTE: `state` will be `AGENT_EXECUTING`
            self._log.debug('=== out: %s', task['stdout'])
            self._log.debug('result_cb  %s: %s [%s] [%s]',
                            task['uid'],
                            task['state'],
                            sorted(task['stdout']),
                            task['return_value'])

            print('result_cb %s: %s %s %s' % (task['uid'], task['state'],
                                              task['stdout'],
                                              task['return_value']))


    # --------------------------------------------------------------------------
    #
    def state_cb(self, tasks):

        for task in tasks:
            uid = task['uid']

            if uid.startswith(self._uid + '.task.m.'):
                self._collected[rp.TASK_EXECUTABLE] += 1


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # This master script runs as a task within a pilot allocation.  The purpose
    # of this master is to (a) spawn a set or workers within the same
    # allocation, (b) to distribute work items (`hello` function calls) to those
    # workers, and (c) to collect the responses again.
    cfg_fname    = str(sys.argv[1])
    cfg          = ru.Config(cfg=ru.read_json(cfg_fname))
    cfg.rank     = int(sys.argv[2])

    n_workers  = cfg.n_workers
    nodes_pw   = cfg.nodes_pw
    cpn        = cfg.cpn
    gpn        = cfg.gpn
    descr      = cfg.worker_descr
    pwd        = os.getcwd()

    # one node is used by master.  Alternatively (and probably better), we could
    # reduce one of the worker sizes by one core.  But it somewhat depends on
    # the worker type and application workload to judge if that makes sense, so
    # we leave it for now.

    # create a master class instance - this will establish communication to the
    # pilot agent
    master = MyMaster(cfg)

    # insert `n` worker tasks into the agent.  The agent will schedule (place)
    # those workers and execute them.  Insert one smaller worker (see above)
    # NOTE: this assumes a certain worker size / layout
    print('workers: %d' % n_workers)
    descr['cpu_processes'] = nodes_pw * cpn
    descr['gpu_processes'] = nodes_pw * gpn
    master.submit_workers(descr=descr, count=n_workers)

    # wait until `m` of those workers are up
    # This is optional, work requests can be submitted before and will wait in
    # a work queue.
  # master.wait(count=nworkers)

    master.start()
    master.submit()
    master.join()
    master.stop()

    # simply terminate
    # FIXME: clean up workers


# ------------------------------------------------------------------------------

