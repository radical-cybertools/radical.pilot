#!/usr/bin/env python3

import os
import sys
import time

from collections import defaultdict

import radical.utils as ru
import radical.pilot as rp


# This script has to run as a task within a pilot allocation, and is
# a demonstration of a task overlay within the RCT framework. It is expected
# to be staged and launched by the `raptor.py` script in the radical.pilot
# examples/misc directory.
# This master task will:
#
#   - create a master which bootstraps a specific communication layer
#   - insert n workers into the pilot (again as a task)
#   - perform RPC handshake with those workers
#   - send RPC requests to the workers
#   - terminate the worker
#
# The worker itself is an external program which is not covered in this code.

RANKS  = 1


@rp.pythontask
def func_mpi(comm, msg, sleep):
    # pylint: disable=reimported
    import time
    print('hello %d/%d: %s' % (comm.rank, comm.size, msg))
    time.sleep(sleep)



@rp.pythontask
def func_non_mpi(a, sleep):
    # pylint: disable=reimported
    import math
    import random
    import time
    b = random.random()
    t = math.exp(a * b)
    time.sleep(sleep)
    print('hello! exp(%f) = %f' % (a * b, t))
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
        self._submitted = defaultdict(int)
        self._collected = defaultdict(int)

        # initialize the task overlay base class.  That base class will ensure
        # proper communication channels to the pilot agent.
        super().__init__(cfg=cfg)

        self._sleep = self._cfg.sleep


    # --------------------------------------------------------------------------
    #
    def submit(self):

        self._prof.prof('create_start')

        # create additional tasks to be distributed to the workers.

        tds = list()
        for i in range(2):

            tds.append(rp.TaskDescription({
                'uid'        : 'task.exe.m.%06d' % i,
                'mode'       : rp.TASK_EXECUTABLE,
                'scheduler'  : None,
                'ranks'      : RANKS,
                'executable' : '/bin/sh',
                'arguments'  : ['-c', 'sleep %d;' % self._sleep +
                             'echo "hello $RP_RANK/$RP_RANKS: $RP_TASK_ID"']}))

            tds.append(rp.TaskDescription({
                'uid'       : 'task.call.m.%06d' % i,
              # 'timeout'   : 10,
                'mode'      : rp.TASK_FUNCTION,
                'ranks'     : RANKS,
                'function'  : 'hello_mpi',
                'kwargs'          : {'msg': 'task.call.m.%06d' % i,
                                     'sleep': self._sleep},
                'scheduler' : 'master.000000'}))

            bson = func_mpi(None, msg='task.call.m.%06d' % i, sleep=self._sleep)
            tds.append(rp.TaskDescription({
                'uid'       : 'task.mpi_ser_func.m.%06d' % i,
              # 'timeout'   : 10,
                'mode'      : rp.TASK_FUNCTION,
                'ranks'     : RANKS,
                'function'  : bson,
                'scheduler' : 'master.000000'}))

            bson = func_non_mpi(i + 1, sleep=self._sleep)
            tds.append(rp.TaskDescription({
                'uid'       : 'task.ser_func.m.%06d' % i,
              # 'timeout'   : 10,
                'mode'      : rp.TASK_FUNCTION,
                'ranks'     : 1,
                'function'  : bson,
                'scheduler' : 'master.000000'}))

            tds.append(rp.TaskDescription({
                'uid'       : 'task.eval.m.%06d' % i,
              # 'timeout'   : 10,
                'mode'      : rp.TASK_EVAL,
                'ranks'     : RANKS,
                'code'      :
                    'print("hello %%s/%%s: %%s [%%s]" %% (os.environ["RP_RANK"],'
                    'os.environ["RP_RANKS"], os.environ["RP_TASK_ID"],'
                    'time.sleep(%d)))' % self._sleep,
                'scheduler' : 'master.000000'}))

            tds.append(rp.TaskDescription({
                'uid'       : 'task.exec.m.%06d' % i,
              # 'timeout'   : 10,
                'mode'      : rp.TASK_EXEC,
                'ranks'     : RANKS,
                'code'      :
                    'import time\ntime.sleep(%d)\n' % self._sleep +
                    'import os\nprint("hello %s/%s: %s" % (os.environ["RP_RANK"],'
                    'os.environ["RP_RANKS"], os.environ["RP_TASK_ID"]))',
                'scheduler' : 'master.000000'}))

            tds.append(rp.TaskDescription({
                'uid'       : 'task.proc.m.%06d' % i,
              # 'timeout'   : 10,
                'mode'      : rp.TASK_PROC,
                'ranks'     : RANKS,
                'executable': '/bin/sh',
                'arguments' : ['-c',
                               'sleep %d; ' % self._sleep +
                               'echo "hello $RP_RANK/$RP_RANKS: '
                               '$RP_TASK_ID"'],
                'scheduler' : 'master.000000'}))

            tds.append(rp.TaskDescription({
                'uid'       : 'task.shell.m.%06d' % i,
              # 'timeout'   : 10,
                'mode'      : rp.TASK_SHELL,
                'ranks'     : RANKS,
                'command'   : 'sleep %d; ' % self._sleep +
                              'echo "hello $RP_RANK/$RP_RANKS: $RP_TASK_ID"',
                'scheduler' : 'master.000000'}))


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
            uid  = task['description']['uid']

            self._submitted[mode] += 1

            # for each `function` mode task, submit one more `proc` mode request
            if mode == rp.TASK_FUNCTION:
                self.submit_tasks(rp.TaskDescription(
                    {'uid'       : 'extra' + uid,
                   # 'timeout'   : 10,
                     'mode'      : rp.TASK_PROC,
                     'ranks'     : RANKS,
                     'executable': '/bin/sh',
                     'arguments' : ['-c',
                                    'sleep %d; ' % self._sleep +
                                    'echo "hello $RP_RANK/$RP_RANKS: '
                                          '$RP_TASK_ID"'],
                     'scheduler' : 'master.000000'}))

        return tasks


    # --------------------------------------------------------------------------
    #
    def result_cb(self, tasks):
        '''
        Log results.

        Log file is named by the master tasks UID.
        '''
        for task in tasks:

            mode = task['description']['mode']
            self._collected[mode] += 1

            # NOTE: `state` will be `AGENT_EXECUTING`
            self._log.info('result_cb  %s: %s [%s] [%s]',
                            task['uid'],
                            task['state'],
                            task['stdout'],
                            task['return_value'])

            # Note that stdout is part of the master task result.
            print('id: %s [%s]:\n    out: %s\n    ret: %s\n'
                 % (task['uid'], task['state'], task['stdout'],
                    task['return_value']))


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

    n_workers        = cfg.n_workers
    nodes_per_worker = cfg.nodes_per_worker
    cores_per_node   = cfg.cores_per_node
    gpus_per_node    = cfg.gpus_per_node
    descr            = cfg.worker_descr
    pwd              = os.getcwd()

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
    descr['ranks']         = nodes_per_worker * cores_per_node
    descr['gpus_per_rank'] = nodes_per_worker * gpus_per_node
    master.submit_workers(descr=descr, count=n_workers)

    # wait until `m` of those workers are up
    # This is optional, work requests can be submitted before and will wait in
    # a work queue.
  # master.wait(count=nworkers)

    master.start()
    master.submit()
    master.join()
    master.stop()


# ------------------------------------------------------------------------------
