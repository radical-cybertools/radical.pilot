

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


        if register:
            self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)
            self.register_publisher(rpc.CONTROL_PUBSUB)

            time.sleep(1)

            # `info` is a placeholder for any additional meta data communicated
            # to the master.
            self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_register',
                                              'arg': {'uid' : self._cfg['uid'],
                                                      'info': self._info}})
        self.enable_bulk_start = True


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def run(fpath, cname, cfg):

        # load worker class from fname if that is a valid string
        wclass = None

        # Create the worker class and run it's work loop.
        if fpath and fpath != 'None':
            wclass = rpu.load_class(fpath, cname, Worker)

        else:
            # import all known workers into the local name space so that
            # `get_type` has a chance to find them
            from .worker_default import DefaultWorker
            from .worker_mpi_am  import MPIWorker

            wclass = rpu.get_type(cname)

        if not wclass:
            raise RuntimeError('no worker [%s] [%s]' % (cname, fpath))

        worker = wclass(cfg)
        worker.start()
        worker.join()


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

        while not self._term.wait(timeout=1.0):
            pass


# ------------------------------------------------------------------------------

