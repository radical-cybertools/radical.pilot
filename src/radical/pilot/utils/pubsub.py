
import zmq
import time
import errno
import pprint
import msgpack

import threading         as mt
import radical.utils     as ru

from .bridge import Bridge
from .misc   import hostip as rpu_hostip


# --------------------------------------------------------------------------
#
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
        except zmq.ContextTerminated as e:
            return None
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
# Notifications between components are based on pubsub channels.  Those channels
# have different scope (bound to the channel name).  Only one specific topic is
# predefined: 'state' will be used for unit state updates.
#
class Pubsub(Bridge):

    def __init__(self, cfg, session):

        super(Pubsub, self).__init__(cfg, session)

        self._initialize_bridge()


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._uid

    @property
    def uid(self):
        return self._uid

    @property
    def channel(self):
        return self._channel


    # --------------------------------------------------------------------------
    # 
    def _initialize_bridge(self):

        self._log.info('start bridge %s', self._uid)

        self._addr       = 'tcp://*:*'

        self._ctx        = zmq.Context()  # rely on the GC destroy the context
        self._in         = self._ctx.socket(zmq.XSUB)
        self._in.linger  = _LINGER_TIMEOUT
        self._in.hwm     = _HIGH_WATER_MARK
        self._in.bind(self._addr)

        self._out        = self._ctx.socket(zmq.XPUB)
        self._out.linger = _LINGER_TIMEOUT
        self._out.hwm    = _HIGH_WATER_MARK
        self._out.bind(self._addr)

        # communicate the bridge ports to the parent process
        _addr_in  = self._in.getsockopt (zmq.LAST_ENDPOINT)
        _addr_out = self._out.getsockopt(zmq.LAST_ENDPOINT)

        # store addresses
        self._addr_in  = ru.Url(_addr_in)
        self._addr_out = ru.Url(_addr_out)

        # use the local hostip for bridge addresses
        self._addr_in.host  = rpu_hostip()
        self._addr_out.host = rpu_hostip()

        self._log.info('bound bridge %s to %s : %s', 
                       self._uid, _addr_in, _addr_out)

        self._log.info('bridge in  on  %s: %s'  % (self._uid, self._addr_in ))
        self._log.info('       out on  %s: %s'  % (self._uid, self._addr_out))

        # start polling for messages
        self._poll = zmq.Poller()
        self._poll.register(self._in,  zmq.POLLIN)
        self._poll.register(self._out, zmq.POLLIN)

        # the bridge runs in a daemon thread, so that the main process will not
        # wait for it.  But, give Python's thread performance (or lack thereof),
        # this means that the user of this class should create a separate
        # process instance to host the bridge thread.
        self._bridge_thread = mt.Thread(target=self._bridge_work)
        self._bridge_thread.daemon = True
        self._bridge_thread.start()

        # inform clients about the bridge, no that the sockets are connected and
        # work is about to start.
        with open('%s/%s.url' % (self._pwd, self.uid), 'w') as fout:
            fout.write('PUB %s\n' % self._addr_in)
            fout.write('SUB %s\n' % self._addr_out)


    # --------------------------------------------------------------------------
    # 
    def wait(self, timeout=None):
        '''
        join negates the daemon thread settings, in that it stops us from
        killing the parent process w/o hanging it.  So we do a slow pull on the
        thread state.
        '''

        start = time.time()

        while True:

            if not self._bridge_thread.is_alive():
                return True

            if  timeout is not None and \
                timeout < time.time() - start:
                return False

            time.sleep(0.1)


    # --------------------------------------------------------------------------
    # 
    def _bridge_work(self):

        # we could use a zmq proxy - but we rather code it directly to have
        # proper logging, timing, etc.  But the code for the proxy would be:
        #
        #     zmq.proxy(socket_in, socket_out)
        #
        # That's the equivalent of the code below.

        try:

            while True:

                _socks = dict(_uninterruptible(self._poll.poll, timeout=1000))
                # timeout in ms

                if self._in in _socks:

                    # if any incoming socket signals a message, get the
                    # message on the subscriber channel, and forward it
                    # to the publishing channel, no questions asked.
                    msg = _uninterruptible(self._in.recv_multipart, flags=zmq.NOBLOCK)
                    _uninterruptible(self._out.send_multipart, msg)

                  # if self._debug:
                  #     self._log.debug('>> %s %s', self.channel, msg)

                if self._out in _socks:
                    # if any outgoing socket signals a message, it's
                    # likely a topic subscription.  We forward that on
                    # the incoming channels to subscribe for the
                    # respective messages.
                    msg = _uninterruptible(self._out.recv_multipart)
                    _uninterruptible(self._in.send_multipart, msg)

                  # if self._debug:
                  #     self._log.debug('<< %s %s', self.channel, msg)

        except Exception:
            self._log.exception('bridge failed')

        # thread ends here


# ------------------------------------------------------------------------------
#
class Publisher(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, channel, session=None):

        self._channel = channel
        self._session = session

        if self._session:
            self._pwd = self._session.get_session_sandbox()
        else:
            self._pwd = '.'

        self._uid = ru.generate_id('%s.pub.%s' % (self._channel, '%(counter)04d'),
                                   ru.ID_CUSTOM)
        self._log = ru.Logger(name=self._uid, level='DEBUG')  ## FIXME

        # avoid superfluous logging calls in critical code sections
        if self._log.getEffectiveLevel() == 10:  # logging.DEBUG:
            self._debug  = True
        else:
            self._debug  = False

        # get addr from bridge.url
        bridge_uid = ru.generate_id("%s" % self._channel, ru.ID_CUSTOM)

        with open('%s/%s.url' % (self._pwd, bridge_uid), 'r') as fin:
            for line in fin.readlines():
                elems = line.split()
                if elems and elems[0] == 'PUB':
                    self._addr = elems[1]
                    break

        self._log.info('connect pub to %s: %s'  % (bridge_uid, self._addr))

        self._ctx      = zmq.Context()  # rely on the GC destroy the context
        self._q        = self._ctx.socket(zmq.PUB)
        self._q.linger = _LINGER_TIMEOUT
        self._q.hwm    = _HIGH_WATER_MARK
        self._q.connect(self._addr)


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._uid

    @property
    def uid(self):
        return self._uid

    @property
    def channel(self):
        return self._channel


    # --------------------------------------------------------------------------
    #
    def put(self, topic, msg):

        assert(isinstance(msg,dict)), 'invalide message type'

        topic = topic.replace(' ', '_')
        data  = msgpack.packb(msg) 

        if self._debug:
            self._log.debug('-> %s %s', self.channel, msg)

        msg = [topic, data]
        _uninterruptible(self._q.send_multipart, msg)


# ------------------------------------------------------------------------------
#
class Subscriber(object):

    def __init__(self, channel, session=None):

        self._channel = channel
        self._session = session

        if self._session:
            self._pwd = self._session.get_session_sandbox()
        else:
            self._pwd = '.'

        self._uid = ru.generate_id('%s.sub.%s' % (self._channel, '%(counter)04d'),
                                    ru.ID_CUSTOM)
        self._log = ru.Logger(name=self._uid, level='DEBUG') ## FIXME

        # avoid superfluous logging calls in critical code sections
        if self._log.getEffectiveLevel() == 10:  # logging.DEBUG:
            self._debug  = True
        else:
            self._debug  = False


        # get addr from bridge.url
        with open('%s/%s.url' % (self._pwd, self._channel), 'r') as fin:
            for line in fin.readlines():
                elems = line.split()
                if elems and elems[0] == 'SUB':
                    self._addr = elems[1]
                    break

        self._log.info('connect sub to %s: %s'  % (self._channel, self._addr))

        self._ctx      = zmq.Context()  # rely on the GC destroy the context
        self._q        = self._ctx.socket(zmq.SUB)
        self._q.linger = _LINGER_TIMEOUT
        self._q.hwm    = _HIGH_WATER_MARK
        self._q.connect(self._addr)


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._uid

    @property
    def uid(self):
        return self._uid

    @property
    def channel(self):
        return self._channel


    # --------------------------------------------------------------------------
    #
    def subscribe(self, topic):

        topic = topic.replace(' ', '_')

        self._log.debug("~~ %s", topic)
        _uninterruptible(self._q.setsockopt, zmq.SUBSCRIBE, topic)


    # --------------------------------------------------------------------------
    #
    def get(self):

        # FIXME: add timeout to allow for graceful termination

        topic, data = _uninterruptible(self._q.recv_multipart)
        msg         = msgpack.unpackb(data) 

      # if self._debug:
      #     self._log.debug('<- %s %s', self.channel, msg)

        return [topic, msg]


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None):  # timeout in ms

        if _uninterruptible(self._q.poll, flags=zmq.POLLIN, timeout=timeout):

            data  = _uninterruptible(self._q.recv_multipart, flags=zmq.NOBLOCK)
            topic = None

            # we run into an intermitant failure mode where the topic element of
            # the multipart message is duplicated, but only when a *previous*
            # receive got interrupted.  We correct this here
            # FIXME: understand *why* this happens
            if isinstance(data, list):
                if len(data) == 2:
                    topic, data = data[0], data[1]
                elif len(data) == 3 and data[0] == data[1]:
                    self._log.error('====== XXX 1 %s', pprint.pformat(data))
                    topic, data = data[1], data[2]

            if not topic or not data:
                return [None, None]

            msg = msgpack.unpackb(data)

          # if self._debug:
          #     self._log.debug('<- %s %s', self.channel, msg)

            return [topic, msg]

        else:
            return [None, None]


# ------------------------------------------------------------------------------

