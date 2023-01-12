
import os
import sys
import time

import threading         as mt

import radical.utils     as ru

from .. import constants as rpc


# ------------------------------------------------------------------------------
#
class Worker(object):
    '''
    Implement the Raptor protocol for dispatching multiple Tasks on persistent
    resources.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, manager, rank):

        if cfg is None:
            cfg = dict()

        if isinstance(cfg, str): self._cfg = ru.Config(cfg=ru.read_json(cfg))
        else                   : self._cfg = ru.Config(cfg=cfg)

        self._rank = rank
        self._sbox = os.environ['RP_TASK_SANDBOX']
        self._uid  = os.environ['RP_TASK_ID']

        self._log  = ru.Logger(name=self._uid,   ns='radical.pilot.worker',
                               level='DEBUG', targets=['.'], path=self._sbox)
        self._prof = ru.Profiler(name=self._uid, ns='radical.pilot.worker',
                                 path=self._sbox)

        # register for lifetime management messages on the control pubsub
        psbox = os.environ['RP_PILOT_SANDBOX']
        ctrl_cfg = ru.read_json('%s/%s.cfg' % (psbox, rpc.CONTROL_PUBSUB))

        self._control_sub = ru.zmq.Subscriber(rpc.CONTROL_PUBSUB,
                                              url=ctrl_cfg['sub'],
                                              log=self._log,
                                              prof=self._prof,
                                              cb=self._control_cb)

        # we push hertbeat and registration messages on that pubsub also
        self._ctrl_pub = ru.zmq.Publisher(rpc.CONTROL_PUBSUB,
                                          url=ctrl_cfg['pub'],
                                          log=self._log,
                                          prof=self._prof)
        # let ZMQ settle
        time.sleep(1)

        # the manager (rank 0) registers the worker with the master
        if manager:

            self._ctrl_pub.put(rpc.CONTROL_PUBSUB, {'cmd': 'worker_register',
                                                    'arg': {'uid' : self._uid}})


          # # FIXME: we never unregister on termination
          # self._ctrl_pub.put(rpc.CONTROL_PUBSUB, {'cmd': 'worker_unregister',
          #                                         'arg': {'uid' : self._uid}})

        # run heartbeat thread in all ranks (one hb msg every `n` seconds)
        self._hb_delay  = 5
        self._hb_thread = mt.Thread(target=self._hb_worker)
        self._hb_thread.daemon = True
        self._hb_thread.start()

        self._log.debug('heartbeat thread started %s:%s', self._uid, self._rank)


    # --------------------------------------------------------------------------
    #
    def _hb_worker(self):

        while True:

            self._ctrl_pub.put(rpc.CONTROL_PUBSUB,
                    {'cmd': 'worker_rank_heartbeat',
                     'arg': {'uid' : self._uid,
                             'rank': self._rank}})

            time.sleep(self._hb_delay)


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):

        if msg['cmd'] == 'terminate':
            self.stop()
            self.join()
            sys.exit()

        elif msg['cmd'] == 'worker_terminate':
            self._log.debug('worker_terminate signal')

            if msg['arg']['uid'] == self._uid:
                self.stop()
                self.join()
                sys.exit()


    # --------------------------------------------------------------------------
    #
    def get_master(self):
        '''
        The worker can submit tasks back to the master - this method will
        return a small shim class to provide that capability.  That class has
        a single method `run_task` which accepts a single `rp.TaskDescription`
        from which a `rp.Task` is created and executed.  The call then waits for
        the task's completion before returning it in a dict representation, the
        same as when passed to the master's `result_cb`.

        Note: the `run_task` call is running in a separate thread and will thus
              not block the master's progress.
        '''

        # ----------------------------------------------------------------------
        class Master(object):

            def __init__(self, addr):
                self._task_service_ep = ru.zmq.Client(url=addr)

            def run_task(self, td):
                return self._task_service_ep.request('run_task', td)
        # ----------------------------------------------------------------------

        return Master(self._cfg.ts_addr)


    # --------------------------------------------------------------------------
    #
    def start(self):

        raise NotImplementedError('`start()` must be implemented by child class')


    # --------------------------------------------------------------------------
    #
    def stop(self):

        raise NotImplementedError('`run()` must be implemented by child class')


    # --------------------------------------------------------------------------
    #
    def join(self):

        raise NotImplementedError('`join()` must be implemented by child class')


# ------------------------------------------------------------------------------

