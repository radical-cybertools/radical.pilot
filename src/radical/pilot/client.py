
__copyright__ = "Copyright 2024, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import time

from functools   import partial
from collections import defaultdict

import radical.utils as ru

from .        import constants as rpc
from .session import Session


# ------------------------------------------------------------------------------
#
class Client(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid=None, reg_addr=None):

        self._uid      = uid      or os.environ.get('RP_SESSION_ID')
        self._reg_addr = reg_addr or os.environ.get('RP_REGISTRY_ADDRESS')

        if not self._uid:
            raise ValueError('no session ID given nor found in env')

        if not self._reg_addr:
            raise ValueError('no registry address given nor found in env')

        self._session = Session(uid=self._uid,
                                _role=Session._CLIENT,
                                _reg_addr=self._reg_addr)

        self._log  = self._session._get_logger('radical.client')
        self._prof = self._session._get_profiler('radical.client')

        self._session._reg.dump('client')

        # hook into control and state pubsubs.  On messages, invoke registered
        # callbacks
        self._ctrl_cbs   = defaultdict(list)
        self._state_cbs  = defaultdict(list)
        self._pubsub_cbs = defaultdict(list)
        self._queue_cbs  = defaultdict(list)

        # ----------------------------------------------------------------------
        # control pubsub
        bname    = 'bridges.%s' % rpc.CONTROL_PUBSUB
        ctrl_cfg = ru.Config(self._session._reg[bname])
        self._ctrl_pub = ru.zmq.Publisher(channel=rpc.CONTROL_PUBSUB,
                                          url=ctrl_cfg.addr_pub,
                                          log=self._log,
                                          prof=self._prof)
        self._ctrl_sub = ru.zmq.Subscriber(channel=rpc.CONTROL_PUBSUB,
                                           url=ctrl_cfg.addr_sub,
                                           log=self._log,
                                           prof=self._prof)


        # ----------------------------------------------------------------------
        # state pubsub (no publisher!)
        bname     = 'bridges.%s' % rpc.STATE_PUBSUB
        state_cfg = ru.Config(self._session._reg[bname])
        self._state_sub = ru.zmq.Subscriber(channel=rpc.STATE_PUBSUB,
                                            url=state_cfg.addr_sub,
                                            log=self._log,
                                            prof=self._prof)


        # ----------------------------------------------------------------------
        # client pubsub
        bname      = 'bridges.%s' % rpc.CLIENT_PUBSUB
        pubsub_cfg = ru.Config(self._session._reg[bname])
        self._pub  = ru.zmq.Publisher(channel=rpc.CLIENT_PUBSUB,
                                      url=pubsub_cfg.addr_pub,
                                      log=self._log,
                                      prof=self._prof)
        self._sub = ru.zmq.Subscriber(channel=rpc.CLIENT_PUBSUB,
                                      url=pubsub_cfg.addr_sub,
                                      log=self._log,
                                      prof=self._prof)

        # ----------------------------------------------------------------------
        # client queue
        bname       = 'bridges.%s' % rpc.CLIENT_QUEUE
        queue_cfg   = ru.Config(self._session._reg[bname])
        self._putter = ru.zmq.Putter(channel=rpc.CLIENT_QUEUE,
                                     url=queue_cfg.addr_put,
                                     log=self._log,
                                     prof=self._prof)
        self._getter = ru.zmq.Getter(channel=rpc.CLIENT_QUEUE,
                                     url=queue_cfg.addr_get,
                                     log=self._log,
                                     prof=self._prof)

        # let zmq settle
        time.sleep(1.1)


    # --------------------------------------------------------------------------
    #
    # control pubsub
    #
    def _ctrl_cb(self, topic, msg):

        self._log.debug('control msg %s: %s', topic, msg)
        for cb in self._ctrl_cbs.get(topic, []):
            try:
                ret = cb(topic, msg)
                if ret is False:
                    self._ctrl_cbs[topic].remove(cb)
            except:
                self._log.exception('control cb %s failed' % repr(cb))


    # --------------------------------------------------------------------------
    #
    def register_ctrl_cb(self, topic, cb):

        self._log.debug('register ctrl cb %s: %s', topic, cb)
        self._ctrl_cbs[topic].append(cb)


    # --------------------------------------------------------------------------
    #
    def subscribe_ctrl_topic(self, topic, cb=None):

        if not topic:
            topic = rpc.CONTROL_PUBSUB

        self._log.debug('subscribe to %s', topic)
        self._ctrl_sub.subscribe(topic, self._ctrl_cb)

        if cb:
            self.register_ctrl_cb(topic, cb)


    # --------------------------------------------------------------------------
    #
    def send_ctrl_msg(self, topic, msg):

        self._log.debug('send ctrl msg %s: %s', topic, msg)
        self._ctrl_pub.put(topic, msg)


    # --------------------------------------------------------------------------
    #
    # state pubsub
    #
    def _state_cb(self, topic, msg):

        self._log.debug_5('client state msg %s: %s', topic, msg)
        for cb in self._state_cbs.get(topic, []):
            try:
                ret = cb(topic, msg)
                if ret is False:
                    self._state_cbs[topic].remove(cb)
            except:
                self._log.exception('state cb %s failed' % repr(cb))


    # --------------------------------------------------------------------------
    #
    def subscribe_state_topic(self, topic=None, cb=None):

        if not topic:
            topic = rpc.STATE_PUBSUB

        self._state_sub.subscribe(topic, self._state_cb)

        if cb:
            self.register_state_cb(topic, cb)


    # --------------------------------------------------------------------------
    #
    def register_state_cb(self, topic, cb):

        self._state_cbs[topic].append(cb)


    # --------------------------------------------------------------------------
    #
    # client pubsub
    #
    def _pubsub_cb(self, topic, msg):

        self._log.debug('pubsub msg %s: %s', topic, msg)
        for cb in self._pubsub_cbs.get(topic, []):
            try:
                ret = cb(topic, msg)
                if ret is False:
                    self._pubsub_cbs[topic].remove(cb)
            except:
                self._log.exception('pubsub cb %s failed' % repr(cb))


    # --------------------------------------------------------------------------
    #
    def register_pubsub_cb(self, topic, cb):

        self._pubsub_cbs[topic].append(cb)


    # --------------------------------------------------------------------------
    #
    def subscribe_pubsub_topic(self, topic, cb=None):

        if not topic:
            topic = 'default'

        self._sub.subscribe(topic, self._pubsub_cb)

        if cb:
            self.register_pubsub_cb(topic, cb)


    # --------------------------------------------------------------------------
    #
    def send_pubsub_msg(self, topic, msg):

        self._pub.put(topic, msg)


    # --------------------------------------------------------------------------
    #
    # client queue
    #
    def _queue_cb(self, qname, msg):

        self._log.debug('queue msg %s: %s', qname, msg)
        for cb in self._queue_cbs.get(qname, []):
            try:
                ret = cb(qname, msg)
                if ret is False:
                    self._queue_cbs[qname].remove(cb)
            except:
                self._log.exception('queue cb %s failed' % repr(cb))


    # --------------------------------------------------------------------------
    #
    def register_queue_cb(self, qname, cb):

        # bind class callback to queue name
        self._queue_cbs[qname].append(cb)


    # --------------------------------------------------------------------------
    #
    def subscribe_queue_topic(self, qname, cb=None):

        if not qname:
            qname = 'default'

        bound = partial(self._queue_cb, qname)
        self._getter.subscribe(qname, bound)

        if cb:
            self.register_queue_cb(qname, cb)


    # --------------------------------------------------------------------------
    #
    def send_queue_msg(self, qname, msg):

        self._putter.put(qname, msg)


# ------------------------------------------------------------------------------

