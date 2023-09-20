
__copyright__ = 'Copyright 2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import io
import sys
import queue

import threading as mt

import radical.utils   as ru

from ..constants import CONTROL_PUBSUB
from ..messages  import RPCRequestMessage, RPCResultMessage


# ------------------------------------------------------------------------------
#
class RPCHelper(object):
    '''
    This class implements a simple synchronous RPC mechanism. It only requires
    the addresses of the control pubsub to use.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self, owner, ctrl_addr_pub, ctrl_addr_sub, log, prof):

        self._owner    = owner          # used for uid scope
        self._addr_pub = ctrl_addr_pub
        self._addr_sub = ctrl_addr_sub

        self._log      = log
        self._prof     = prof

        self._active   = None
        self._queue    = queue.Queue()
        self._lock     = mt.Lock()
        self._handlers = dict()

        self._pub = ru.zmq.Publisher(channel=CONTROL_PUBSUB,
                                     url=self._addr_pub,
                                     log=self._log,
                                     prof=self._prof)

        self._thread = mt.Thread(target=self._work)
        self._thread.daemon = True
        self._thread.start()


    # --------------------------------------------------------------------------
    #
    def request(self, cmd, *args, **kwargs):

        rid = ru.generate_id('%s.rpc' % self._owner)
        req = RPCRequestMessage(uid=rid, cmd=cmd, args=args, kwargs=kwargs)

        self._active = rid

        self._pub.put(CONTROL_PUBSUB, req)
        self._log.debug_3('sent rpc req %s', req)

        res = self._queue.get()

        assert res.uid == req.uid

        if res.exc:
            # FIXME: try to deserialize exception type
            #        this should work at least for standard exceptions
            raise RuntimeError(str(res.exc))

        return res


    # --------------------------------------------------------------------------
    #
    def _work(self):

        pub = ru.zmq.Publisher(channel=CONTROL_PUBSUB,
                               url=self._addr_pub,
                               log=self._log,
                               prof=self._prof)

        sub = ru.zmq.Subscriber(channel=CONTROL_PUBSUB,
                                topic=CONTROL_PUBSUB,
                                url=self._addr_sub,
                                log=self._log,
                                prof=self._prof)
        sub.subscribe(CONTROL_PUBSUB)

        import time
        time.sleep(1)

        while True:

            data = sub.get_nowait(100)
            if not data or data == [None, None]:
                continue

            msg_topic = data[0]
            msg_data  = data[1]

            if not isinstance(msg_data, dict):
                continue

            try:
                msg = ru.zmq.Message.deserialize(msg_data)

            except Exception as e:
                # not a `ru.zmq.Message` type
                continue

            if isinstance(msg, RPCRequestMessage):

                # handle any RPC requests for which a handler is registered
                self._log.debug_2('got rpc req: %s', msg)

                with self._lock:
                    if msg.cmd in self._handlers:
                        rep = self.handle_request(msg)
                        pub.put(CONTROL_PUBSUB, rep)
                    else:
                        self._log.debug_2('no rpc handler for %s', msg.cmd)

            elif isinstance(msg, RPCResultMessage):

                # collect any RPC response whose uid matches the one we wait for

                self._log.debug_2('got rpc res', self._active, msg.uid)
                if self._active and msg.uid == self._active:
                    self._active = None
                    self._queue.put(msg)


    # --------------------------------------------------------------------------
    #
    def add_handler(self, cmd, handler):
        '''
        register a handler for the specified rpc command type
        '''

        with self._lock:

            if cmd in self._handlers:
                raise ValueError('handler for rpc cmd %s already set' % cmd)

            self._handlers[cmd] = handler


    # --------------------------------------------------------------------------
    #
    def del_handler(self, cmd):
        '''
        unregister a handler for the specified rpc command type
        '''

        with self._lock:

            if cmd not in self._handlers:
                raise ValueError('handler for rpc cmd %s not set' % cmd)

            del self._handlers[cmd]


# ------------------------------------------------------------------------------
