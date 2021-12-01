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
    def submit_tasks(self):

        self._prof.prof('create_start')

        world_size = self._cfg.n_masters
        rank       = self._cfg.rank

        # create an initial list of work items to be distributed to the workers.
        # Work items MUST be serializable dictionaries.
        idx   = rank
        total = int(eval(self._cfg.workload.total))                       # noqa
        while idx < total:

            uid  = 'request.eval.%06d' % idx
            item = {'uid'  :   uid,
                    'mode' :  'eval',
                    'cores':  1,
                  # 'gpus' :  1,
                    'data' : {
                        'code': 'print("hello world")'
                    }}
            self.request(item)


            uid  = 'request.exec.%06d' % idx
            item = {'uid'  :   uid,
                    'mode' :  'exec',
                    'cores':  1,
                  # 'gpus' :  1,
                    'data' : {
                        'pre_exec': 'import time',
                        'code'    : 'print("hello stdout"); return "hello world"'
                    }}
            self.request(item)


            uid  = 'request.call.%06d' % idx
            item = {'uid'  :   uid,
                    'mode' :  'call',
                    'cores':  1,
                  # 'gpus' :  1,
                    'data' : {'method': 'hello',
                              'kwargs': {'count': idx,
                                         'uid'  : uid}
                   }}
            self.request(item)


            uid  = 'request.proc.%06d' % idx
            item = {'uid'  :   uid,
                    'mode' :  'proc',
                    'cores':  1,
                  # 'gpus' :  1,
                    'data' : {'exe' : '/bin/echo',
                              'args': ['hello', 'world']
                   }}
            self.request(item)


            uid  = 'request.shell.%06d' % idx
            item = {'uid'  :   uid,
                    'mode' :  'shell',
                    'cores':  1,
                  # 'gpus' :  1,
                    'data' : {'env' : {'WORLD': 'world'},
                              'cmd' : '/bin/echo "hello $WORLD"'
                   }}
            self.request(item)

            idx += world_size

        self._prof.prof('create_stop')


    # --------------------------------------------------------------------------
    #
    def request_cb(self, requests):

        for req in requests:

            self._log.debug('=== request_cb %s\n' % (req['uid']))

            # for each `shell` mode request, submit one more `proc` mode request
            if req['mode'] == 'call':

                uid  = 'request.extra.%06d' % self._cnt
                item = {'uid'  :   uid,
                        'mode' :  'proc',
                        'cores':  1,
                        'data' : {'exe' : '/bin/echo',
                                  'args': ['hello', 'world']
                       }}
                self.request(item)
                self._cnt += 1

        # return the original request for execution
        return requests


    # --------------------------------------------------------------------------
    #
    def result_cb(self, req):

        self._log.debug('=== result_cb  %s: %s [%s]\n'
                       % (req.uid, req.state, req.result))


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
    master.submit(descr=descr, count=n_workers, cores=cpn, gpus=gpn)

    # wait until `m` of those workers are up
    # This is optional, work requests can be submitted before and will wait in
    # a work queue.
  # master.wait(count=nworkers)

    master.start()
    master.submit_tasks()
    master.join()
    master.stop()

    # simply terminate
    # FIXME: clean up workers


# ------------------------------------------------------------------------------

