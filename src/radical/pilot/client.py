
__copyright__ = "Copyright 2024, http://radical.rutgers.edu"
__license__   = "MIT"

import os

import radical.utils as ru

from .        import constants as rpc
from .session import Session


# ------------------------------------------------------------------------------
#
class Client(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid=None, reg_addr=None):

        print('===== client init: %s: %s' % (reg_addr,
                                             os.environ.get('RP_REGISTRY_ADDRESS')))

        self._uid      = uid      or os.environ.get('RP_SESSION_ID')
        self._reg_addr = reg_addr or os.environ.get('RP_REGISTRY_ADDRESS')

        if not self._uid:
            raise ValueError('no session ID given nor found in env')

        if not self._reg_addr:
            raise ValueError('no registry address given nor found in env')

        self._session = Session(uid=self._uid,
                                _role=Session._CLIENT,
                                _reg_addr=self._reg_addr)

        self._log     = self._session._get_logger('radical.client')
        self._prof    = self._session._get_profiler('radical.client')

        self._log.debug('===== client: %s: %s', self._reg_addr,
                        self._session._reg['bridges.control_pubsub']['addr_pub'])


        self._log = ru.Logger('radical.pilot.client', targets=['-'], level='DEBUG')


        # hook into control and state pubsubs.  On messages, invoke registered
        # callbacks
        ctrl_cfg       = ru.Config(self._session._reg['bridges.control_pubsub'])
        self._log.debug('=== %s', ctrl_cfg)
        self._ctrl_cbs = list()
        self._ctrl_pub = ru.zmq.Publisher(channel=rpc.CONTROL_PUBSUB,
                                          url=ctrl_cfg['addr_pub'],
                                          log=self._log,
                                          prof=self._prof)
        ru.zmq.Subscriber(channel='control_pubsub',
                          topic='control_pubsub',
                          url=ctrl_cfg.addr_sub,
                          cb=self._control_cb,
                          log=self._log,
                          prof=self._prof)


        state_cfg       = ru.Config(self._session._reg['bridges.state_pubsub'])
        self._state_cbs = list()
        ru.zmq.Subscriber(channel='state_pubsub',
                          topic='state_pubsub',
                          url=state_cfg.addr_sub,
                          cb=self._state_cb,
                          log=self._log,
                          prof=self._prof)


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):

        self._log.debug('client control msg %s: %s', topic, msg)
        for cb in self._ctrl_cbs:
            try:
                cb(topic, msg)
            except:
                self._log.exception('control cb %s failed' % repr(cb))


    # --------------------------------------------------------------------------
    #
    def register_control_cb(self, cb):

        self._ctrl_cbs.append(cb)


    # --------------------------------------------------------------------------
    #
    def send_control_msg(self, msg):

        self._ctrl_pub.put('control_pubsub', msg)


    # --------------------------------------------------------------------------
    #
    def _state_cb(self, topic, msg):

        self._log.debug_5('client state msg %s: %s', topic, msg)
        for cb in self._state_cbs:
            try:
                cb(topic, msg)
            except:
                self._log.exception('state cb %s failed' % repr(cb))


    # --------------------------------------------------------------------------
    #
    def register_state_cb(self, cb):

        self._state_cbs.append(cb)


# ------------------------------------------------------------------------------

