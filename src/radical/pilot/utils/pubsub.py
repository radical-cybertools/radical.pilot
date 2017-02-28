
import os
import sys
import zmq
import copy
import math
import time
import errno
import pprint
import signal
import msgpack

import Queue           as pyq
import setproctitle    as spt
import multiprocessing as mp

import radical.utils   as ru


# --------------------------------------------------------------------------
# defines for pubsub roles
#
PUBSUB_PUB    = 'pub'
PUBSUB_SUB    = 'sub'
PUBSUB_BRIDGE = 'bridge'
PUBSUB_ROLES  = [PUBSUB_PUB, PUBSUB_SUB, PUBSUB_BRIDGE]

_USE_MULTIPART   = False  # send [topic, data] as multipart message
_BRIDGE_TIMEOUT  =     1  # how long to wait for bridge startup
_LINGER_TIMEOUT  =   250  # ms to linger after close
_HIGH_WATER_MARK =     0  # number of messages to buffer before dropping


# --------------------------------------------------------------------------
#
# zmq will (rightly) barf at interrupted system calls.  We are able to rerun
# those calls.
#
# FIXME: how does that behave wrt. tomeouts?  We probably should include
#        an explicit timeout parameter.
#
# kudos: https://gist.github.com/minrk/5258909
#
def _uninterruptible(f, *args, **kwargs):
    cnt = 0
    while True:
        cnt += 1
        try:
            return f(*args, **kwargs)
        except zmq.ZMQError as e:
            if e.errno == errno.EINTR:
                if cnt > 10:
                    raise
                # interrupted, try again
                continue
            else:
                # real error, raise it
                raise

# ------------------------------------------------------------------------------
# 
# NOTE: see docs/architecture/component_termination.py
#       for info about mp.Process and signal handling
# 
def sigterm_handler(signum, frame):
    raise KeyboardInterrupt('sigterm')

def sigusr2_handler(signum, frame):
    raise KeyboardInterrupt('sigusr2')


# ==============================================================================
#
# Notifications between components are based on pubsub channels.  Those channels
# have different scope (bound to the channel name).  Only one specific topic is
# predefined: 'state' will be used for unit state updates.
#
class Pubsub(mp.Process):

    # NOTE: see docs/architecture/component_termination.py
    #       for info about mp.Process method overloading

    def __init__(self, session, channel, role, cfg, addr=None):
        """
        Addresses are of the form 'tcp://host:port'.  Both 'host' and 'port' can
        be wildcards for BRIDGE roles -- the bridge will report the in and out
        addresses as obj.bridge_in and obj.bridge_out.
        """

        self._session = session
        self._channel = channel
        self._role    = role
        self._cfg     = copy.deepcopy(cfg)
        self._addr    = addr

        if self._role not in PUBSUB_ROLES:
            raise ValueError("unknown role '%s' (%s)" % (self._role, PUBSUB_ROLES))

        mp.Process.__init__(self)

        self._name = "%s.%s" % (self._channel.replace('_', '.'), self._role)
        self._log  = self._session._get_logger(self._name, 
                                               level=self._cfg.get('log_level', 'debug'))

        # avoid superfluous logging calls in critical code sections
        if self._log.getEffectiveLevel() == 10: # logging.DEBUG:
            self._debug = True
        else:
            self._debug = False

        self._q          = None           # the zmq queue
        self._bridge_in  = None           # bridge input  addr
        self._bridge_out = None           # bridge output addr

        if not self._addr:
            self._addr = 'tcp://*:*'

        self._log.info("create %s - %s - %s", self._channel, self._role, self._addr)


        # ----------------------------------------------------------------------
        # behavior depends on the role...
        if self._role == PUBSUB_PUB:

            ctx = zmq.Context()
            self._q = ctx.socket(zmq.PUB)
            self._q.linger = _LINGER_TIMEOUT
            self._q.hwm    = _HIGH_WATER_MARK
            self._q.connect(self._addr)


        # ----------------------------------------------------------------------
        elif self._role == PUBSUB_BRIDGE:

            # we expect bridges to always use a port wildcard. Make sure
            # that's the case
            elems = self._addr.split(':')
            if len(elems) > 2 and elems[2] and elems[2] != '*':
                raise RuntimeError('wildcard port (*) required for bridge addresses (%s)' \
                                % self._addr)

            self._pqueue = mp.Queue()
            self.start()

            try:
                self._bridge_in, self._bridge_out = self._pqueue.get(True, _BRIDGE_TIMEOUT)

            except pyq.Empty as e:
                raise RuntimeError ("bridge did not come up! (%s)" % e)


        # ----------------------------------------------------------------------
        elif self._role == PUBSUB_SUB:

            ctx = zmq.Context()
            self._q = ctx.socket(zmq.SUB)
            self._q.linger = _LINGER_TIMEOUT
            self._q.hwm    = _HIGH_WATER_MARK
            self._q.connect(self._addr)


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._name

    @property
    def channel(self):
        return self._channel

    @property
    def role(self):
        return self._role

    @property
    def addr(self):
        return self._addr

    @property
    def bridge_in(self):
        assert(self._role == PUBSUB_BRIDGE)
        return self._bridge_in

    @property
    def bridge_out(self):
        assert(self._role == PUBSUB_BRIDGE)
        return self._bridge_out


    # --------------------------------------------------------------------------
    #
    def poll(self):
        """
        This is a wrapper around is_alive() which mimics the behavior of the same
        call in the subprocess.Popen class with the same name.  It does not
        return an exitcode though, but 'None' if the process is still
        alive, and always '0' otherwise
        """
        if self.is_alive():
            return None
        else:
            return 0


    # --------------------------------------------------------------------------
    #
    def join(self, timeout=None):

        try:
            mp.Process.join(self)

        except AssertionError as e:
            self._log.warn('assert on join ignored')


    # --------------------------------------------------------------------------
    # 
    def stop(self):

        try:
            # only terminate if started and alive
            if self.pid and self.is_alive():
                self.terminate()
                mp.Process.join(self, timeout=1)
            
            # make sure its dead
            if self.is_alive():
                raise RuntimeError('Cannot kill child %s' % self.pid)

        except OSError as e:
            self._log.warn('OSError on stop ignored')

        except AttributeError as e:
            self._log.warn('AttributeError on stop ignored')


    # --------------------------------------------------------------------------
    # 
    def run(self):

        assert(self._role == PUBSUB_BRIDGE)

        signal.signal(signal.SIGTERM, sigterm_handler)
        signal.signal(signal.SIGUSR2, sigusr2_handler)

        try:
            spt.setproctitle('rp.%s' % self._name)
            self._log.info('start bridge %s on %s', self._name, self._addr)

            ctx = zmq.Context()
            _in = ctx.socket(zmq.XSUB)
            _in.linger = _LINGER_TIMEOUT
            _in.hwm    = _HIGH_WATER_MARK
            _in.bind(self._addr)

            _out = ctx.socket(zmq.XPUB)
            _out.linger = _LINGER_TIMEOUT
            _out.hwm    = _HIGH_WATER_MARK
            _out.bind(self._addr)

            # communicate the bridge ports to the parent process
            _in_port  =  _in.getsockopt(zmq.LAST_ENDPOINT)
            _out_port = _out.getsockopt(zmq.LAST_ENDPOINT)

            self._pqueue.put([_in_port, _out_port])

            self._log.info('bound bridge %s to %s : %s', self._name, _in_port, _out_port)

            # start polling for messages
            _poll = zmq.Poller()
            _poll.register(_in,  zmq.POLLIN)
            _poll.register(_out, zmq.POLLIN)

            while True:

                _socks = dict(_uninterruptible(_poll.poll, timeout=1000)) # timeout in ms

                if _in in _socks:
                    
                    # if any incoming socket signals a message, get the
                    # message on the subscriber channel, and forward it
                    # to the publishing channel, no questions asked.
                    if _USE_MULTIPART:
                        msg = _uninterruptible(_in.recv_multipart, flags=zmq.NOBLOCK)
                        _uninterruptible(_out.send_multipart, msg)
                    else:
                        msg = _uninterruptible(_in.recv, flags=zmq.NOBLOCK)
                        _uninterruptible(_out.send, msg)
                  # if self._debug:
                  #     self._log.debug("-> %s", pprint.pformat(msg))


                if _out in _socks:
                    # if any outgoing socket signals a message, it's
                    # likely a topic subscription.  We forward that on
                    # the incoming channels to subscribe for the
                    # respective messages.
                    if _USE_MULTIPART:
                        msg = _uninterruptible(_out.recv_multipart)
                        _uninterruptible(_in.send_multipart, msg)
                    else:
                        msg = _uninterruptible(_out.recv)
                        _uninterruptible(_in.send, msg)
                  # if self._debug:
                  #     self._log.debug("<- %s", pprint.pformat(msg))

        except Exception as e:
            self._log.exception('bridge error: %s', e)

        except SystemExit:
            self._log.warn('bridge exit')
       
        except KeyboardInterrupt:
            self._log.warn('bridge intr')
       
        finally:
            self._log.debug('bridge final')


    # --------------------------------------------------------------------------
    #
    def subscribe(self, topic):

        if not self._role == PUBSUB_SUB:
            raise RuntimeError("channel %s (%s) can't subscribe()" % (self._channel, self._role))

        topic = topic.replace(' ', '_')

      # self._log.debug("~~ %s", topic)
        _uninterruptible(self._q.setsockopt, zmq.SUBSCRIBE, topic)


    # --------------------------------------------------------------------------
    #
    def put(self, topic, msg):

        if not self._role == PUBSUB_PUB:
            self._log.debug("!> role mismatch")
            raise RuntimeError("channel %s (%s) can't put()" % (self._channel, self._role))

        if not isinstance(msg,dict):
            self._log.error("not a dict message: \n%s\n\n%s\n\n",
                    pprint.pformat(msg),
                    ru.get_stacktrace())
            raise RuntimeError('ill formated publication on %s' % topic)

      # self._log.debug("?> %s" % pprint.pformat(msg))

        topic = topic.replace(' ', '_')
        data  = msgpack.packb(msg) 

        if _USE_MULTIPART:
          # if self._debug:
          #     self._log.debug("-> %s", ([topic, pprint.pformat(msg)]))
            _uninterruptible(self._q.send_multipart, [topic, data])

        else:
          # if self._debug:
          #     self._log.debug("-> %s %s", topic, pprint.pformat(msg))
            _uninterruptible(self._q.send, "%s %s" % (topic, data))


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._role == PUBSUB_SUB:
            raise RuntimeError("channel %s (%s) can't get()" % (self._channel, self._role))

        if _USE_MULTIPART:
            topic, data = _uninterruptible(self._q.recv_multipart)

        else:
            raw = _uninterruptible(self._q.recv)
            topic, data = raw.split(' ', 1)

        msg = msgpack.unpackb(data) 
      # if self._debug:
      #     self._log.debug("<- %s", ([topic, pprint.pformat(msg)]))
        return [topic, msg]


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None): # timeout in ms

        if not self._role == PUBSUB_SUB:
            raise RuntimeError("channel %s (%s) can't get_nowait()" % (self._channel, self._role))

        if _uninterruptible(self._q.poll, flags=zmq.POLLIN, timeout=timeout):

            if _USE_MULTIPART:
                topic, data = _uninterruptible(self._q.recv_multipart, flags=zmq.NOBLOCK)

            else:
                raw = _uninterruptible(self._q.recv)
                topic, data = raw.split(' ', 1)

            msg = msgpack.unpackb(data) 
          # if self._debug:
          #     self._log.debug("<< %s", ([topic, pprint.pformat(msg)]))
            return [topic, msg]

        else:
            return [None, None]


# ------------------------------------------------------------------------------

