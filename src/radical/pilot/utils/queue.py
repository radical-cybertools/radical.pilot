
import os
import sys
import zmq
import copy
import math
import time
import errno
import pprint
import msgpack

import Queue           as pyq
import setproctitle    as spt
import threading       as mt
import multiprocessing as mp

import radical.utils   as ru

from .misc import hostip as rpu_hostip


# --------------------------------------------------------------------------
# defines for queue roles
#
QUEUE_INPUT   = 'input'
QUEUE_BRIDGE  = 'bridge'
QUEUE_OUTPUT  = 'output'
QUEUE_ROLES   = [QUEUE_INPUT, QUEUE_BRIDGE, QUEUE_OUTPUT]

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
# Communication between components is done via queues.  The semantics we expect
# (and which is what is matched by the native Python Queue.Queue), is:
#
#   - multiple upstream   components put messages onto the same queue (input)
#   - multiple downstream components get messages from the same queue (output)
#   - local order of messages is maintained
#   - message routing is fair: whatever downstream component calls 'get' first
#     will get the next message (bridge)
#
# Additionally, we require Queues to be uni-directional, ie. Queues have an
# in-end for which one can call 'put()', and and out-end, for which one can call
# 'get()'.  We implement the interface of Queue.Queue:
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
# the same identifier, they will get the same queue instance.


# ==============================================================================
#
class Queue(ru.Process):

    def __init__(self, session, qname, role, cfg, addr=None):
        """
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
        addresses as obj.addr_in and obj.addr_out.  """

        self._session = session
        self._qname   = qname
        self._role    = role
        self._cfg     = copy.deepcopy(cfg)
        self._addr    = addr

        assert(self._role in QUEUE_ROLES), 'invalid role %s' % self._role

        self._uid = "%s.%s" % (self._qname.replace('_', '.'), self._role)
        self._uid = ru.generate_id(self._uid)
        self._log = self._session._get_logger(name=self._uid, 
                         level=self._cfg.get('log_level'))

        super(Queue, self).__init__(name=self._uid, log=self._log)

        # avoid superfluous logging calls in critical code sections
        if self._log.getEffectiveLevel() == 10: # logging.DEBUG:
            self._debug = True
        else:
            self._debug = False

        self._lock       = mt.RLock()     # for _requested
        self._requested  = False          # send/recv sync
        self._addr_in    = None           # bridge input  addr
        self._addr_out   = None           # bridge output addr
        self._stall_hwm  = cfg.get('stall_hwm', 1)
        self._bulk_size  = cfg.get('bulk_size', 1)

        if not self._addr:
            self._addr = 'tcp://*:*'

        self._log.info("create %s - %s - %s", self._qname, self._role, self._addr)

        self._q    = None           # the zmq queue
        self._in   = None
        self._out  = None
        self._ctx  = None


        # ----------------------------------------------------------------------
        # behavior depends on the role...
        if self._role == QUEUE_INPUT:

            self._ctx = zmq.Context()
            self._session._to_destroy.append(self._ctx)

            self._q   = self._ctx.socket(zmq.PUSH)
            self._q.linger = _LINGER_TIMEOUT
            self._q.hwm    = _HIGH_WATER_MARK
            self._q.connect(self._addr)
            self.start(spawn=False)


        # ----------------------------------------------------------------------
        elif self._role == QUEUE_BRIDGE:

            # we expect bridges to always use a port wildcard. Make sure
            # that's the case
            elems = self._addr.split(':')
            if len(elems) > 2 and elems[2] and elems[2] != '*':
                raise RuntimeError('wildcard port (*) required for bridge addresses (%s)' \
                                % self._addr)

            self._pqueue = mp.Queue()
            self.start()

            try:
                [addr_in, addr_out] = self._pqueue.get(True, _BRIDGE_TIMEOUT)

                # store addresses
                self._addr_in  = ru.Url(addr_in)
                self._addr_out = ru.Url(addr_out)

                # use the local hostip for bridge addresses
                self._addr_in.host  = rpu_hostip()
                self._addr_out.host = rpu_hostip()

            except pyq.Empty as e:
                raise RuntimeError ("bridge did not come up! (%s)" % e)


        # ----------------------------------------------------------------------
        elif self._role == QUEUE_OUTPUT:

            self._ctx = zmq.Context()
            self._session._to_destroy.append(self._ctx)

            self._q   = self._ctx.socket(zmq.REQ)
            self._q.linger = _LINGER_TIMEOUT
            self._q.hwm    = _HIGH_WATER_MARK
            self._q.connect(self._addr)
            self.start(spawn=False)


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._uid

    @property
    def uid(self):
        return self._uid

    @property
    def qname(self):
        return self._qname

    @property
    def role(self):
        return self._role

    @property
    def addr(self):
        return self._addr

    @property
    def addr_in(self):
        assert(self._role == QUEUE_BRIDGE), 'addr_in only set on bridges'
        return self._addr_in

    @property
    def addr_out(self):
        assert(self._role == QUEUE_BRIDGE), 'addr_out only set on bridges'
        return self._addr_out


    # --------------------------------------------------------------------------
    # 
    def ru_initialize_child(self):

        assert(self._role == QUEUE_BRIDGE), 'only bridges can be started'

        self._uid = self._uid + '.child'
        self._log = self._session._get_logger(name=self._uid, 
                         level=self._cfg.get('log_level'))

        spt.setproctitle('rp.%s' % self._uid)
        self._log.info('start bridge %s on %s', self._uid, self._addr)

        # FIXME: should we cache messages coming in at the pull/push 
        #        side, so as not to block the push end?

        self._ctx = zmq.Context()
        self._session._to_destroy.append(self._ctx)

        self._in = self._ctx.socket(zmq.PULL)
        self._in.linger = _LINGER_TIMEOUT
        self._in.hwm    = _HIGH_WATER_MARK
        self._in.bind(self._addr)

        self._out = self._ctx.socket(zmq.REP)
        self._out.linger = _LINGER_TIMEOUT
        self._out.hwm    = _HIGH_WATER_MARK
        self._out.bind(self._addr)

        # communicate the bridge ports to the parent process
        _addr_in  = self._in.getsockopt( zmq.LAST_ENDPOINT)
        _addr_out = self._out.getsockopt(zmq.LAST_ENDPOINT)

        self._pqueue.put([_addr_in, _addr_out])

        self._log.info('bound bridge %s to %s : %s', self._uid, _addr_in, _addr_out)

        # start polling for messages
        self._poll = zmq.Poller()
        self._poll.register(self._out, zmq.POLLIN)


    # --------------------------------------------------------------------------
    # 
    def ru_finalize_common(self):

        if self._q   : self._q   .close()
        if self._in  : self._in  .close()
        if self._out : self._out .close()
        if self._ctx : self._ctx.destroy()


    # --------------------------------------------------------------------------
    # 
    def work_cb(self):

        # We wait for an incoming message.  When one is received,
        # we'll poll all outgoing sockets for requests, and the
        # forward the message to whoever requested it.
        #
        # If so configured, we can stall messages until reaching
        # a certain high-water-mark, and upon reaching that will
        # release all messages at once.  When stalling for such set
        # of messages, we wait for self._stall_hwm messages, and
        # then forward those to whatever output channel requesting
        # them (individually),
        # FIXME: we may want to release all stalled messages after
        #        some (configurable) timeout?
        # FIXME: is it worth introducing a 'flush' command or
        #        message type?
        # NOTE:  hwm collection interferes with termination:
        #        while we wait for messages to arrive, we will not leave this
        #        work_cb, and thus will not allow the `ru.Process` main loop to
        #        terminate gracefully as needed.  One solution would be to
        #        inject manual statements to check for termination conditions.
        #        The other option would be to collect the messages across
        #        multiple work_cb invocations -- but the latter options would
        #        significantly increase the code complexity, as we would need to
        #        maintain state across method invocations, so we opt for the
        #        first option.
        hwm  = self._stall_hwm
        bulk = self._bulk_size

        msgs = list()
        while len(msgs) < hwm:
            data = None
            while self.is_alive(strict=False):
                if _uninterruptible(self._in.poll, flags=zmq.POLLIN, 
                                   timeout=1000):
                    data = _uninterruptible(self._in.recv)
                    break
            if not data:
                if not self.is_alive(strict=False):
                    self._log.warn('not alive anymore?')
                    return False
            msg = msgpack.unpackb(data) 
            if isinstance(msg, list): 
                msgs += msg
            else: 
                msgs.append(msg)
            self._log.debug('stall %s/%s', len(msgs), hwm)
        self._log.debug('hwm   %s/%s', len(msgs), hwm)

        # if 'bulk' is '0', we send all messages as
        # a single bulk.  Otherwise, we chop them up
        # into bulks of the given size
        if bulk <= 0:
            nbulks = 1
            bulks  = [msgs]
        else:
            nbulks = int(math.ceil(len(msgs) / float(bulk)))
            bulks  = ru.partition(msgs, nbulks)

        while bulks:
            # timeout in ms
            events = dict(_uninterruptible(self._poll.poll, 1000))

            if self._out in events:

                req  = _uninterruptible(self._out.recv)
                data = msgpack.packb(bulks.pop(0)) 
                _uninterruptible(self._out.send, data)

                # go to next message/bulk (break while loop)
                self._log.debug('sent  %s [hwm: %s]', (nbulks-len(bulks)), hwm)

        return True


    # --------------------------------------------------------------------------
    #
    def put(self, msg):

        if not self._role == QUEUE_INPUT:
            raise RuntimeError("queue %s (%s) can't put()" % (self._qname, self._role))

      # if self._debug:
      #     self._log.debug("-> %s", pprint.pformat(msg))
        data = msgpack.packb(msg) 
        _uninterruptible(self._q.send, data)


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._role == QUEUE_OUTPUT:
            raise RuntimeError("queue %s (%s) can't get()" % (self._qname, self._role))

        _uninterruptible(self._q.send, 'request')

        data = _uninterruptible(self._q.recv)
        msg  = msgpack.unpackb(data) 
      # if self._debug:
      #     self._log.debug("<- %s", pprint.pformat(msg))
        return msg


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None): # timeout in ms

        if not self._role == QUEUE_OUTPUT:
            raise RuntimeError("queue %s (%s) can't get_nowait()" % (self._qname, self._role))

        with self._lock: # need to protect self._requested

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

