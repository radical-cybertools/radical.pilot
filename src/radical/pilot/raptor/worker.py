

import io
import os
import sys
import time
import queue

import threading         as mt
import multiprocessing   as mp

import radical.utils     as ru

from .. import Session
from .. import utils     as rpu
from .. import constants as rpc


# ------------------------------------------------------------------------------
#
class Worker(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, register=True, session=None):

        self._session = session

        if cfg is None:
            cfg = dict()

        if isinstance(cfg, str): cfg = ru.Config(cfg=ru.read_json(cfg))
        else                   : cfg = ru.Config(cfg=cfg)

        cfg.uid    = os.environ['RP_TASK_ID']
        self._info = ru.Config(cfg=cfg.get('info', {}))

        if not self._session:
            self._session = Session(cfg=cfg, uid=cfg.sid, _primary=False)

        rpu.Component.__init__(self, cfg, self._session)

        # connect to master
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        # connect to the request / response ZMQ queues
        self._res_put = ru.zmq.Putter('to_res', self._info.res_addr_put)
        self._req_get = ru.zmq.Getter('to_req', self._info.req_addr_get,
                                                cb=self.request_cb)

        # make sure that channels are up before registering
        time.sleep(1)

        # `info` is a placeholder for any additional meta data communicated to
        # the master.  Only first rank publishes.
        if register:
            self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_register',
                                              'arg': {'uid' : self._cfg['uid'],
                                                      'info': self._info}})
        self.enable_bulk_start = True


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def run(fpath, cname, cfg):

        # Create the worker class and run it's work loop.
        wclass = rpu.load_class(fpath, cname, Worker)
        worker = wclass(cfg)
        worker.start()
        worker.join()


    # --------------------------------------------------------------------------
    #
    def request_cb(self, tasks):
        '''
        the request callback MUST be implemented by inheriting classes
        '''

        raise NotImplementedError('`request_cb` not implemented')


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):

        if msg['cmd'] == 'terminate':
            self._term.set()

        elif msg['cmd'] == 'worker_terminate':
            if msg['arg']['uid'] == self._cfg['uid']:
                self._term.set()


    # --------------------------------------------------------------------------
    #
    def start(self):

        # note that this overwrites `Component.start()` - this worker component
        # is not using the registered input channels, but listens to it's own
        # set of channels in `request_cb`.
        pass


    # --------------------------------------------------------------------------
    #
    def join(self):

        while not self._term.is_set():
            time.sleep(1.0)


# ------------------------------------------------------------------------------

