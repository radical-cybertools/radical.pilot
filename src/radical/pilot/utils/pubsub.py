
import os
import zmq
import json
import time
import errno
import pprint
import signal
import Queue           as pyq
import multiprocessing as mp
import radical.utils   as ru

# --------------------------------------------------------------------------
# defines for pubsub roles
PUBSUB_PUB    = 'pub'
PUBSUB_SUB    = 'sub'
PUBSUB_BRIDGE = 'bridge'
PUBSUB_ROLES  = [PUBSUB_PUB, PUBSUB_SUB, PUBSUB_BRIDGE]

# defines for pubsub types
PUBSUB_ZMQ    = 'zmq'
PUBSUB_TYPES  = [PUBSUB_ZMQ]

_USE_MULTIPART  = False # send [topic, data] as multipart message
_BRIDGE_TIMEOUT = 5.0   # how long to wait for bridge startup


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
                print 'interrupted! [%s] [%s] [%s]' % (f, args, kwargs)
                continue
            else:
                # real error, raise it
                raise

# ==============================================================================
#
# Notifications between components are based on pubsub channels.  Those channels
# have different scope (bound to the channel name).  Only one specific topic is
# predefined: 'state' will be used for unit state updates.
#
class Pubsub(object):
    """
    This is a factory for pubsub endpoints.
    """

    def __init__(self, flavor, channel, role, address=None):
        """
        Addresses are of the form 'tcp://host:port'.  Both 'host' and 'port' can
        be wildcards for BRIDGE roles -- the bridge will report the in and out
        addresses as obj.bridge_in and obj.bridge_out.
        """

        self._flavor     = flavor
        self._channel    = channel
        self._role       = role
        self._addr       = address
        self._debug      = False
        self._name       = "pubsub.%s.%s" % (self._channel, self._role)
        self._log        = ru.get_logger('rp.bridges', target="%s.log" % self._name)
        self._bridge_in  = None           # bridge input  addr
        self._bridge_out = None           # bridge output addr

        if not self._addr:
            self._addr = 'tcp://*:*'

        self._log.info("create %s - %s - %s", self._channel, self._role, self._addr)

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


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Pubsub.
    #
    @classmethod
    def create(cls, flavor, channel, role, address=None):

        # Make sure that we are the base-class!
        if cls != Pubsub:
            raise TypeError("Pubsub Factory only available to base class!")

        try:
            impl = {
                PUBSUB_ZMQ     : PubsubZMQ,
            }[flavor]
          # print 'instantiating %s' % impl
            return impl(flavor, channel, role, address)
        except KeyError:
            raise RuntimeError("Pubsub type '%s' unknown!" % flavor)


    # --------------------------------------------------------------------------
    #
    @property
    def bridge_in(self):
        if self._role != PUBSUB_BRIDGE:
            raise TypeError('bridge_in is only defined on a bridge')
        return self._bridge_in


    # --------------------------------------------------------------------------
    #
    @property
    def bridge_out(self):
        if self._role != PUBSUB_BRIDGE:
            raise TypeError('bridge_out is only defined on a bridge')
        return self._bridge_out


    # --------------------------------------------------------------------------
    #
    def poll(self):
        """
        check state of endpoint or bridge
        None: RUNNING
        0   : DONE
        1   : FAILED
        """
        return None


    # --------------------------------------------------------------------------
    #
    def publish(self, topic):
        raise NotImplementedError('publish() is not implemented')


    # --------------------------------------------------------------------------
    #
    def subscribe(self, topic):
        raise NotImplementedError('subscribe() is not implemented')


    # --------------------------------------------------------------------------
    #
    def put(self, topic, msg):
        raise NotImplementedError('put() is not implemented')


    # --------------------------------------------------------------------------
    #
    def get(self):
        raise NotImplementedError('get() is not implemented')


    # --------------------------------------------------------------------------
    #
    def get_nowait(self):
        raise NotImplementedError('getnowait() is not implemented')


    # --------------------------------------------------------------------------
    #
    def stop(self):
        raise NotImplementedError('stop() is not implemented')


# ==============================================================================
#
class PubsubZMQ(Pubsub):

    def __init__(self, flavor, channel, role, address=None):
        """
        This PubSub implementation is built upon, as you may have guessed
        already, the ZMQ pubsub communication pattern.
        """

        self._p = None  # the bridge process

        Pubsub.__init__(self, flavor, channel, role, address)


        # ----------------------------------------------------------------------
        # behavior depends on the role...
        if self._role == PUBSUB_PUB:

            ctx = zmq.Context()
            self._q = ctx.socket(zmq.PUB)
            self._q.connect(str(self._addr))


        # ----------------------------------------------------------------------
        elif self._role == PUBSUB_BRIDGE:

            # we expect bridges to always use a port wildcard. Make sure
            # that's the case
            elems = self._addr.split(':')
            if len(elems) > 2 and elems[2] and elems[2] != '*':
                raise RuntimeError('wildcard port (*) required for bridge addresses (%s)' \
                                % self._addr)

            # ------------------------------------------------------------------
            def _bridge(addr, pqueue):

                try:
                    import setproctitle as spt
                    spt.setproctitle('radical.pilot %s' % self._name)
                except Exception as e:
                    pass

                try:
                    # reset signal handlers to their default
                    signal.signal(signal.SIGINT,  signal.SIG_DFL)
                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)

                    self._log.info('start bridge %s on %s', self._name, addr)

                    ctx = zmq.Context()
                    _in = ctx.socket(zmq.XSUB)
                    _in.bind(addr)

                    _out = ctx.socket(zmq.XPUB)
                    _out.bind(addr)

                    # communicate the bridge ports to the parent process
                    _in_port  =  _in.getsockopt(zmq.LAST_ENDPOINT)
                    _out_port = _out.getsockopt(zmq.LAST_ENDPOINT)

                    pqueue.put([_in_port, _out_port])

                    self._log.info('bound bridge %s to %s : %s', self._name, _in_port, _out_port)

                    # start polling for messages
                    _poll = zmq.Poller()
                    _poll.register(_in,  zmq.POLLIN)
                    _poll.register(_out, zmq.POLLIN)

                    while True:

                        _socks = dict(_uninterruptible(_poll.poll, timeout=1000)) # timeout in ms

                        if _in in _socks:
                            if _USE_MULTIPART:
                                msg = _uninterruptible(_in.recv_multipart, flags=zmq.NOBLOCK)
                                _uninterruptible(_out.send_multipart, msg)
                            else:
                                msg = _uninterruptible(_in.recv, flags=zmq.NOBLOCK)
                                _uninterruptible(_out.send, msg)
                          # self._log.debug("-> %s", msg)


                        if _out in _socks:
                            if _USE_MULTIPART:
                                msg = _uninterruptible(_out.recv_multipart)
                                _uninterruptible(_in.send_multipart, msg)
                            else:
                                msg = _uninterruptible(_out.recv)
                                _uninterruptible(_in.send, msg)
                          # self._log.debug("<- %s", msg)

                except Exception as e:
                    self._log.exception('bridge error: %s', e)
            # ------------------------------------------------------------------

            pqueue   = mp.Queue()
            self._p  = mp.Process(target=_bridge, args=[self._addr, pqueue])
            self._p.start()

            try:
                self._bridge_in, self._bridge_out = pqueue.get(True, _BRIDGE_TIMEOUT)
            except pyq.Empty as e:
                raise RuntimeError ("bridge did not come up! (%s)" % e)

        # ----------------------------------------------------------------------
        elif self._role == PUBSUB_SUB:

            ctx = zmq.Context()
            self._q = ctx.socket(zmq.SUB)
            self._q.connect(self._addr)

        # ----------------------------------------------------------------------
        else:
            raise RuntimeError ("unsupported pubsub role '%s' (%s)" % (self._role, PUBSUB_ROLES))


    # --------------------------------------------------------------------------
    #
    def __del__(self):

        self.stop()


    # --------------------------------------------------------------------------
    #
    def poll(self):
        """
        Only check bridges -- endpoints are otherwise always considered valid
        """
        if self._p and not self._p.is_alive():
            return 0


    # --------------------------------------------------------------------------
    #
    def stop(self):

        if self._p:
            self._p.terminate()


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
            raise RuntimeError("channel %s (%s) can't put()" % (self._channel, self._role))

        topic = topic.replace(' ', '_')
        data = json.dumps(msg)

        if _USE_MULTIPART:
          # self._log.debug("-> %s", str([topic, data]))
            _uninterruptible(self._q.send_multipart, [topic, data])

        else:
          # self._log.debug("-> %s %s", topic, data)
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

        msg = json.loads(data)
      # self._log.debug("<- %s", str([topic, pprint.pformat(msg)]))
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

            msg = json.loads(data)
            self._log.debug(">> %s", str([topic, pprint.pformat(msg)]))
            return [topic, msg]

        else:
            return [None, None]


# ------------------------------------------------------------------------------

