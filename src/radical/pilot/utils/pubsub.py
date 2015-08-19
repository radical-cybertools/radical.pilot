
import os
import zmq
import json
import time
import pprint
import threading       as mt
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

# some predefined port numbers
_PUBSUB_PORTS     = {
        'client_command_pubsub'   : 'tcp://*:11000',
        'agent_command_pubsub'    : 'tcp://*:11002',
        'agent_unschedule_pubsub' : 'tcp://*:11004',
        'agent_reschedule_pubsub' : 'tcp://*:11006',
        'agent_state_pubsub'      : 'tcp://*:11008',
    }

_USE_MULTIPART = False

# --------------------------------------------------------------------------
#
# the pub-to-bridge end of the pubsub uses a different port than the
# bridge-to-sub end...
#
def _port_inc(addr):

    u = ru.Url(addr)
    u.port += 1
    return str(u)


# --------------------------------------------------------------------------
#
# bridges by default bind to all interfaces on a given port, inputs and outputs
# connect to localhost (127.0.0.1)
# bridge-output end...
#
def _get_addr(name, role):

    addr = _PUBSUB_PORTS.get(name)

    if not addr:
        raise LookupError("no addr for pubsub type '%s'" % name)

    if role != PUBSUB_BRIDGE:
        u = ru.Url(addr)
        u.host = '127.0.0.1'
        addr = str(u)

    return addr


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

        self._flavor  = flavor
        self._channel = channel
        self._role    = role
        self._addr    = ru.Url(address)
        self._debug   = False
        self._logfd   = None
        self._name    = "pubsub.%s.%s.%d" % (self._channel, self._role, os.getpid())

        if 'msg' in os.environ.get('RADICAL_DEBUG', '').lower():
            self._debug = True

        # sanity check on address
        default_addr = ru.Url(_get_addr(channel, role))

        # we replace only empty parts of the addr with default values
        if not self._addr       : self._addr        = default_addr
        if not self._addr.schema: self._addr.schema = default_addr.schema
        if not self._addr.host  : self._addr.host   = default_addr.host
        if not self._addr.port  : self._addr.port   = default_addr.port

        if not self._addr:
            raise RuntimeError("no default address found for '%s'" % self._channel)

        self._log ("create %s - %s - %s - %d" \
                % (channel, role, self._addr, os.getpid()))

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
    def _log(self, msg):

        if self._debug:
            if not self._logfd:
                self._logfd = open("%s.log" % self.name, 'a')
            self._logfd.write("%15.5f: %-30s: %s\n" % (time.time(), self._channel, msg))
            self._logfd.flush()


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
    def close(self):
        raise NotImplementedError('close() is not implemented')


# ==============================================================================
#
class PubsubZMQ(Pubsub):

    def __init__(self, flavor, channel, role, address=None):
        """
        This PubSub implementation is built upon, as you may have guessed
        already, the ZMQ pubsub communication pattern.
        """

        Pubsub.__init__(self, flavor, channel, role, address)

        self._p   = None           # the bridge process
        self._ctx = zmq.Context()  # one zmq context suffices

        # zmq checks on address
        if  self._addr.path   != ''    or \
            self._addr.schema != 'tcp' :
            raise ValueError("url '%s' cannot be used for zmq pubsubs" % self._addr)

        if self._addr.port:
            if (self._addr.port % 2):
                raise ValueError("port numbers must be even, not '%d'" % self._addr.port)

        if self._role != PUBSUB_BRIDGE:
            if self._addr.host == '*':
                self._addr.host = '127.0.0.1'

        self._log('%s/%s uses addr %s' % (self._channel, self._role, self._addr))


        # ----------------------------------------------------------------------
        # behavior depends on the role...
        if self._role == PUBSUB_PUB:

            self._q = self._ctx.socket(zmq.PUB)
            self._q.connect(str(self._addr))


        # ----------------------------------------------------------------------
        elif self._role == PUBSUB_BRIDGE:

            # ------------------------------------------------------------------
            def _bridge(ctx, addr_in, addr_out):

              # self._log ('_bridge: %s %s' % (addr_in, addr_out))
                _in = ctx.socket(zmq.XSUB)
                _in.bind(addr_in)

                _out = ctx.socket(zmq.XPUB)
                _out.bind(addr_out)

                _poll = zmq.Poller()
                _poll.register(_in,  zmq.POLLIN)
                _poll.register(_out, zmq.POLLIN)

                while True:

                    _socks = dict(_poll.poll(timeout=1000)) # timeout in ms

                    if _in in _socks:
                        if _USE_MULTIPART:
                            msg = _in.recv_multipart(flags=zmq.NOBLOCK)
                            _out.send_multipart(msg)
                        else:
                            msg = _in.recv(flags=zmq.NOBLOCK)
                            _out.send(msg)
                      # self._log("-> %s" % msg)


                    if _out in _socks:
                        if _USE_MULTIPART:
                            msg = _out.recv_multipart()
                            _in.send_multipart(msg)
                        else:
                            msg = _out.recv()
                            _in.send(msg)
                      # self._log("<- %s" % msg)
            # ------------------------------------------------------------------

            addr_in  = str(self._addr)
            addr_out = str(_port_inc(self._addr))
            self._p  = mp.Process(target=_bridge, args=[self._ctx, addr_in, addr_out])
            self._p.start()

        # ----------------------------------------------------------------------
        elif self._role == PUBSUB_SUB:

            self._q = self._ctx.socket(zmq.SUB)
            self._q.connect(str(_port_inc(self._addr)))

        # ----------------------------------------------------------------------
        else:
            raise RuntimeError ("unsupported pubsub role '%s' (%s)" % (self._role, _PUBSUB_ROLES))


    # --------------------------------------------------------------------------
    #
    def __del__(self):

        self.close()


    # --------------------------------------------------------------------------
    #
    def close(self):

        if self._p:
            self._p.terminate()


    # --------------------------------------------------------------------------
    #
    def subscribe(self, topic):

        if not self._role == PUBSUB_SUB:
            raise RuntimeError("channel %s (%s) can't subscribe()" % (self._channel, self._role))

        topic = topic.replace(' ', '_')

      # self._log("~~ %s" % topic)
        self._q.setsockopt(zmq.SUBSCRIBE, topic)


    # --------------------------------------------------------------------------
    #
    def put(self, topic, msg):

        if not self._role == PUBSUB_PUB:
            raise RuntimeError("channel %s (%s) can't put()" % (self._channel, self._role))

        topic = topic.replace(' ', '_')
        data = json.dumps(msg)

        if _USE_MULTIPART:
          # self._log("-> %s" % str([topic, data]))
            self._q.send_multipart ([topic, data])

        else:
          # self._log("-> %s %s" % (topic, data))
            self._q.send ("%s %s" % (topic, data))


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._role == PUBSUB_SUB:
            raise RuntimeError("channel %s (%s) can't get()" % (self._channel, self._role))

        if _USE_MULTIPART:
            topic, data = self._q.recv_multipart()

        else:
            raw = self._q.recv()
            topic, data = raw.split(' ', 1)

        msg = json.loads(data)
      # self._log("<- %s" % str([topic, pprint.pformat(msg)]))
        return [topic, msg]


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None): # timeout in ms

        if not self._role == PUBSUB_SUB:
            raise RuntimeError("channel %s (%s) can't get_nowait()" % (self._channel, self._role))

        if self._q.poll (flags=zmq.POLLIN, timeout=timeout):

            if _USE_MULTIPART:
                topic, data = self._q.recv_multipart(flags=zmq.NOBLOCK)

            else:
                raw = self._q.recv()
                topic, data = raw.split(' ', 1)

            msg = json.loads(data)
            self._log(">> %s" % str([topic, pprint.pformat(msg)]))
            return [topic, msg]

        else:
            return [None, None]


# ------------------------------------------------------------------------------

