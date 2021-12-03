#!/usr/bin/env python3
# pylint: disable=redefined-outer-name

import os
import sys

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

        # initialize the task overlay base class.  That base class will ensure
        # proper communication channels to the pilot agent.
        rp.raptor.Master.__init__(self, cfg=cfg)


    # --------------------------------------------------------------------------
    #
    def submit(self):

        self._prof.prof('create_start')

        world_size = self._cfg.n_masters
        rank       = self._cfg.rank

        # create an initial list of work items to be distributed to the workers.
        # Work items MUST be serializable dictionaries.
        idx   = rank
        total = int(eval(self._cfg.workload.total))                       # noqa
        while idx < total:

            td = rp.TaskDescription({'uid'        : 'task.eval.%06d' % idx,
                                     'mode'       : rp.RP_EVAL,
                                     'code'       : 'print("hello world")'})
            self.submit_tasks(td)

            td = rp.TaskDescription({'uid'        : 'task.exec.%06d' % idx,
                                     'mode'       : rp.RP_EXEC,
                                     'pre_exec'   : ['import time'],
                                     'code'       : 'print("hello stdout"); '
                                                    'return "hello world"'
                                     })
            self.submit_tasks(td)

            td = rp.TaskDescription({'uid'        : 'task.call.%06d' % idx,
                                     'mode'       : rp.RP_FUNCTION,
                                     'function'   : 'test',
                                     'kwargs'     : {'msg': 'task.call.%06d' % idx}
                                    })
            self.submit_tasks(td)

            td = rp.TaskDescription({'uid'        : 'task.proc.%06d' % idx,
                                     'mode'       : rp.RP_PROC,
                                     'executable' : '/bin/echo',
                                     'arguments'  : ['hello', 'world']
                                    })
            self.submit_tasks(td)

            td = rp.TaskDescription({'uid'        : 'task.shell.%06d' % idx,
                                     'mode'       : rp.RP_SHELL,
                                     'environment': {'WORLD': 'world'},
                                     'command'    : '/bin/echo "hello $WORLD"'
                                    })
            self.submit_tasks(td)

            idx += world_size

        self._prof.prof('create_stop')


    # --------------------------------------------------------------------------
    #
    def request_cb(self, tasks):

        for task in tasks:

            self._log.debug('request_cb %s\n' % (task['uid']))

            # for each `function` mode task, submit one more `proc` mode request
            if task['description']['mode'] == rp.RP_FUNCTION:

                uid  = 'request.extra.%06d' % self._cnt
                td   = rp.TaskDescription({'uid'          : uid,
                                           'mode'         : rp.RP_PROC,
                                           'cpu_processes': 1,
                                           'executable'   : '/bin/echo',
                                           'arguments'    : ['hello', 'world']
                                          })
                self.submit_tasks(td)
                self._cnt += 1

        # return the original request for execution
        return tasks


    # --------------------------------------------------------------------------
    #
    def result_cb(self, task):

        self._log.debug('result_cb  %s: %s [%s] [%s]\n'
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

