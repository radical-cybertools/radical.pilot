
import sys
import time
import threading         as mt

import radical.utils     as ru

from .. import Session
from .. import utils     as rpu
from .. import constants as rpc


# ------------------------------------------------------------------------------
#
class Worker(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        if isinstance(cfg, str): cfg = ru.Config(cfg=ru.read_json(cfg))
        else                   : cfg = ru.Config(cfg=cfg)

        self._uid     = cfg.uid
        self._term    = mt.Event()
        self._info    = ru.Config(cfg=cfg.get('info', {}))
        self._session = Session(cfg=cfg, _primary=False)

        rpu.Component.__init__(self, cfg, self._session)

        # connect to master
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        info = self.initialize()

        self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_register',
                                          'arg': {'uid' : self._uid,
                                                  'info': info}})


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):

        self._log.debug('got control msg: %s: %s', topic, msg)

        if msg['cmd'] == 'worker_terminate':
            if msg['arg']['uid'] == self._uid:
                self._term.set()
                self.stop()
                sys.exit(0)


    # --------------------------------------------------------------------------
    #
    def run(self):

        while not self._term.is_set():
            time.sleep(0.1)


# ------------------------------------------------------------------------------

