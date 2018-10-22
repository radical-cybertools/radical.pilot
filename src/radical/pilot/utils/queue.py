
import zmq
import copy
import math
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
# Communication between components is done via queues.  Queues are
# uni-directional, ie. Queues have an input-end for which one can call 'put()',
# and and output-end, for which one can call 'get()'.
#
# The semantics we expect (and which is what is matched by the native Python
# `Queue.Queue`), is:
#
#   - multiple upstream   components put messages onto the same queue (input)
#   - multiple downstream components get messages from the same queue (output)
#   - local order of messages is maintained: order of messages pushed onto the
#     *same* input is preserved when pulled on any output
#   - message routing is fair: whatever downstream component calls 'get' first
#     will get the next message
#
# We implement the interface of Queue.Queue:
#
#   put(msg)
#   get()
#   get_nowait()
#
# Not implemented is, at the moment:
#
#   qsize
#   empty
#   full 
#   put(msg, block, timeout)
#   put_nowait
#   get(block, timeout)
#   task_done
#
# Our Queue additionally takes 'name', 'role' and 'address' parameter on the
# constructor.  'role' can be 'input', 'bridge' or 'output', where 'input' is
# the end of a queue one can 'put()' messages into, and 'output' the end of the
# queue where one can 'get()' messages from. A 'bridge' acts as as a message
# forwarder.  'address' denominates a connection endpoint, and 'name' is
# a unique identifier: if multiple instances in the current process space use
# the same identifier, they will get the same queue instance (are connected to
# the same bridge).
#
class Queue(Bridge):

    def __init__(self, cfg, session):
        '''
        This Queue type sets up an zmq channel of this kind:

            input \            / output
                   -- bridge -- 
            input /            \ output

        ie. any number of inputs can 'zmq.push()' to a bridge (which
        'zmq.pull()'s), and any number of outputs can 'zmq.request()' 
        messages from the bridge (which 'zmq.response()'s).

        The bridge is the entity which 'bind()'s network interfaces, both input
        and output type endpoints 'connect()' to it.  It is the callees
        responsibility to ensure that only one bridge of a given type exists.

        Addresses are of the form 'tcp://host:port'.  Both 'host' and 'port' can
        be wildcards for BRIDGE roles -- the bridge will report the in and out
        addresses as obj.addr_in and obj.addr_out.
        '''

        super(Queue, self).__init__(cfg, session)

        self._channel    = self._cfg['name']
        self._stall_hwm  = self._cfg.get('stall_hwm', 1)
        self._bulk_size  = self._cfg.get('bulk_size', 1)

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
        self._in         = self._ctx.socket(zmq.PULL)
        self._in.linger  = _LINGER_TIMEOUT
        self._in.hwm     = _HIGH_WATER_MARK
        self._in.bind(self._addr)

        self._out        = self._ctx.socket(zmq.REP)
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
        with open('%s/%s.url' % (self._pwd, self.channel), 'w') as fout:
            fout.write('PUT %s\n' % self._addr_in)
            fout.write('GET %s\n' % self._addr_out)


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

        try:

            while True:

                # We wait for an incoming message.  When one is received,
                # we'll poll all outgoing sockets for requests, and the
                # forward the message to whoever requested it.
                bulk = self._bulk_size
                while not _uninterruptible(self._in.poll, flags=zmq.POLLIN, 
                                           timeout=1000):
                    pass

                data = _uninterruptible(self._in.recv)
                msgs = msgpack.unpackb(data) 

                if not isinstance(msgs, list):
                    msgs = [msgs]

                # if 'bulk' is '0', we send all messages as
                # a single bulk.  Otherwise, we chop them up
                # into bulks of the given size
                if bulk <= 0:
                    nbulks = 1
                    bulks  = [msgs]
                else:
                    nbulks = int(math.ceil(len(msgs) / float(bulk)))
                    bulks  = ru.partition(msgs, nbulks)

                for bulk in bulks:
                    # timeout in ms
                    events = dict(_uninterruptible(self._poll.poll, 1000))

                    if self._out in events:

                        req  = _uninterruptible(self._out.recv)
                        data = msgpack.packb(bulk) 
                        _uninterruptible(self._out.send, data)
                        if self._debug:
                            self._log.debug("<> %s", pprint.pformat(bulk))

        except  Exception:
            self._log.exception('bridge failed')


# ------------------------------------------------------------------------------

class Putter(object):

    def __init__(self, channel, session=None):

        self._channel = channel
        self._session = session

        if self._session:
            self._pwd = self._session.get_session_sandbox()
        else:
            self._pwd = '.'

        self._uid = ru.generate_id('%s.put.%s' % (self._channel, '%(counter)04d'),
                                    ru.ID_CUSTOM)
        self._log = ru.Logger(name=self._uid)

        # avoid superfluous logging calls in critical code sections
        if self._log.getEffectiveLevel() == 10:  # logging.DEBUG:
            self._debug  = True
        else:
            self._debug  = False

        # get addr from bridge.url
        with open('%s/%s.url' % (self._pwd, self._channel), 'r') as fin:
            for line in fin.readlines():
                elems = line.split()
                if elems and elems[0] == 'PUT':
                    self._addr = elems[1]
                    break

        self._log.info('connect put to %s: %s'  % (self._channel, self._addr))

        self._ctx      = zmq.Context()  # rely on the GC destroy the context
        self._q        = self._ctx.socket(zmq.PUSH)
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
    def put(self, msg):

        if self._debug:
            self._log.debug("-> %s", pprint.pformat(msg))
        data = msgpack.packb(msg) 
        _uninterruptible(self._q.send, data)


# ------------------------------------------------------------------------------

class Getter(object):

    def __init__(self, channel, session=None):

        self._channel = channel
        self._session = session

        if self._session:
            self._pwd = self._session.get_session_sandbox()
        else:
            self._pwd = '.'

        self._uid = ru.generate_id('%s.get.%s' % (self._channel, '%(counter)04d'),
                                    ru.ID_CUSTOM)
        self._log = ru.Logger(name=self._uid)

        # avoid superfluous logging calls in critical code sections
        if self._log.getEffectiveLevel() == 10:  # logging.DEBUG:
            self._debug  = True
        else:
            self._debug  = False

        # get addr from bridge.url
        with open('%s/%s.url' % (self._pwd, self._channel), 'r') as fin:
            for line in fin.readlines():
                elems = line.split()
                if elems and elems[0] == 'GET':
                    self._addr = elems[1]
                    break

        self._log.info('connect get to %s: %s'  % (self._channel, self._addr))

        self._lock     = mt.RLock()
        self._requested  = False        # send/recv sync

        self._ctx      = zmq.Context()  # rely on the GC destroy the context
        self._q        = self._ctx.socket(zmq.REQ)
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
    def get(self):

        _uninterruptible(self._q.send, 'request %s' % self._uid)

        data = _uninterruptible(self._q.recv)
        msg  = msgpack.unpackb(data) 

        if self._debug:
            self._log.debug("<- %s", pprint.pformat(msg))

        return msg


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None):  # timeout in ms

        with self._lock:  # need to protect self._requested

            if not self._requested:
                # we can only send the request once per recieval
                _uninterruptible(self._q.send, 'request')
                self._requested = True

          # try:
          #     msg = self._q.recv_json(flags=zmq.NOBLOCK)
          #     self._requested = False
          #     if self._debug:
          #         self._log.debug("<< %s", pprint.pformat(msg))
          #     return msg
          #
          # except zmq.Again:
          #     return None

            if _uninterruptible(self._q.poll, flags=zmq.POLLIN, timeout=timeout):
                data = _uninterruptible(self._q.recv)
                msg  = msgpack.unpackb(data) 
                self._requested = False
              # if self._debug:
              #     self._log.debug("<< %s", pprint.pformat(msg))
                return msg

            else:
                return None


# ------------------------------------------------------------------------------

