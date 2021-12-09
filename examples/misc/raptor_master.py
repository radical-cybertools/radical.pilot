#!/usr/bin/env python3
# pylint: disable=redefined-outer-name

import os
import sys
import copy
import time

import radical.utils as ru
import radical.pilot as rp

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
        self._submitted = {rp.TASK_EXECUTABLE : 0,
                           rp.TASK_FUNCTION   : 0,
                           rp.TASK_EVAL       : 0,
                           rp.TASK_EXEC       : 0,
                           rp.TASK_PROC       : 0,
                           rp.TASK_SHELL      : 0}
        self._collected = {rp.TASK_EXECUTABLE : 0,
                           rp.TASK_FUNCTION   : 0,
                           rp.TASK_EVAL       : 0,
                           rp.TASK_EXEC       : 0,
                           rp.TASK_PROC       : 0,
                           rp.TASK_SHELL      : 0}

        # initialize the task overlay base class.  That base class will ensure
        # proper communication channels to the pilot agent.
        rp.raptor.Master.__init__(self, cfg=cfg)


    # --------------------------------------------------------------------------
    #
    def submit(self):

        self._prof.prof('create_start')

        world_size = self._cfg.n_masters
        rank       = self._cfg.rank
        psbox      = os.environ['RP_PILOT_SANDBOX']

        # create an initial list of work items to be distributed to the workers.
        # Work items MUST be serializable dictionaries.
        idx    = rank
        stop   = time.time() + (self._cfg.workload.runtime * 60)

        self._log.debug('==== submit 1 until %.1f', stop)

        while time.time() < stop:
            self._log.debug('==== submit 2 until %.1f', stop)

            td = rp.TaskDescription({'uid'        : 'task.eval.%06d' % idx,
                                     'mode'       : rp.TASK_EVAL,
                                     'code'       : 'print("hello world")'})
            self.submit_tasks(td)
            self._submitted[rp.TASK_EVAL] += 1

            td = rp.TaskDescription({'uid'        : 'task.exec.%06d' % idx,
                                     'mode'       : rp.TASK_EXEC,
                                     'pre_exec'   : ['import time'],
                                     'code'       : 'print("hello stdout"); '
                                                    'return "hello world"'
                                     })
            self.submit_tasks(td)
            self._submitted[rp.TASK_EXEC] += 1

            td = rp.TaskDescription({'uid'        : 'task.call.%06d' % idx,
                                     'mode'       : rp.TASK_FUNCTION,
                                     'function'   : 'test',
                                     'kwargs'     : {'msg': 'world'}
                                    })
            self.submit_tasks(td)
            self._submitted[rp.TASK_FUNCTION] += 1

            td = rp.TaskDescription({'uid'        : 'task.proc.%06d' % idx,
                                     'mode'       : rp.TASK_PROC,
                                     'executable' : '/bin/echo',
                                     'arguments'  : ['hello', 'world']
                                    })
            self.submit_tasks(td)
            self._submitted[rp.TASK_PROC] += 1

            td = rp.TaskDescription({'uid'        : 'task.shell.%06d' % idx,
                                     'mode'       : rp.TASK_SHELL,
                                     'environment': {'WORLD': 'world'},
                                     'command'    : '/bin/echo "hello $WORLD"'
                                    })
            self.submit_tasks(td)
            self._submitted[rp.TASK_SHELL] += 1

            td = rp.TaskDescription({'uid'        : '%s.task.%06d' % (self._uid, idx),
                                     'mode'       : rp.TASK_EXECUTABLE,
                                     'executable' : '/bin/sh',
                                     'arguments'  : [
                                         '%s/radical-pilot-hello.sh' % psbox
                                     ]
                                    })
            self.submit_tasks(td)
          # self._submitted[rp.TASK_EXECUTABLE] += 1

            idx += world_size

            # slow down if we have too many tasks submitted
            # FIXME: use larger chunks above
            while True:
                completed = sum(self._collected.values())
                submitted = sum(self._submitted.values())

                if completed >= submitted - 2024:
                    self._log.debug('==== submit cont: %d >= %d ',
                                     completed, submitted - 1024)
                    break
                self._log.debug('==== wait   cont: %d >= %d ',
                                     completed, submitted - 1024)

                time.sleep(1)

        self._log.debug('==== submit stopped')

        self._prof.prof('create_stop')

        import pprint
        self._log.debug('==== submitted: %s', pprint.pformat(self._submitted))
        self._log.debug('==== collected: %s', pprint.pformat(self._collected))


        # after runtime is out we wait for the remaining outstanding tasks to
        # complete
        while True:
            completed = sum(self._collected.values())
            submitted = sum(self._submitted.values())

            self._log.debug('==== exec done?: %d >= %d ', completed, submitted)
            if completed >= submitted:
                self._log.debug('==== exec done!')
                self.stop()
                break

            time.sleep(1)

        self._log.debug('==== exec done!!')

    # --------------------------------------------------------------------------
    #
    def request_cb(self, tasks):

        return tasks

      # for task in tasks:
      #
      #     self._log.debug('=== request_cb %s\n' % (task['uid']))
      #
      #     # for each `function` mode task, submit one more `proc` mode request
      #     if task['description']['mode'] == rp.TASK_FUNCTION:
      #
      #         uid  = 'task.extra.%06d' % self._cnt
      #         td   = rp.TaskDescription({'uid'          : uid,
      #                                    'mode'         : rp.TASK_PROC,
      #                                    'cpu_processes': 1,
      #                                    'executable'   : '/bin/echo',
      #                                    'arguments'    : ['hello', 'world']
      #                                   })
      #         self.submit_tasks(td)
      #         self._cnt += 1
      #         self._submitted[rp.TASK_PROC] += 1
      #
      # return tasks


    # --------------------------------------------------------------------------
    #
    def result_cb(self, task):

        completed = sum(self._collected.values())
        submitted = sum(self._submitted.values())
        mode      = task['description']['mode']

        self._collected[mode] += 1
        self._log.debug('=== result_cb  %s: %s [%s] [%s] [%s] [%s]\n',
                       task['uid'], task['state'], task['stdout'],
                       task['return_value'], completed, submitted)


    def state_cb(self, tasks):

        for task in tasks:
            uid = task['uid']

            if uid.startswith(self._uid + '.task.'):
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

