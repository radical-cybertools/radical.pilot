
import os
import zmq
import time
import errno
import pprint
import signal
import Queue           as pyq
import threading       as mt
import multiprocessing as mp
import radical.utils   as ru

# --------------------------------------------------------------------------
# defines for queue roles
QUEUE_INPUT   = 'input'
QUEUE_BRIDGE  = 'bridge'
QUEUE_OUTPUT  = 'output'
QUEUE_ROLES   = [QUEUE_INPUT, QUEUE_BRIDGE, QUEUE_OUTPUT]

# defines for queue types
QUEUE_THREAD  = 'thread'
QUEUE_PROCESS = 'process'
QUEUE_ZMQ     = 'zmq'
QUEUE_TYPES   = [QUEUE_THREAD, QUEUE_PROCESS, QUEUE_ZMQ]

_BRIDGE_TIMEOUT = 5.0  # how long to wait for bridge startup

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
# 'get()'.  This allows us to transparently (and easily) use different
# implementations of the communication channel, including zmq.
#
# The queue implementation we use depends on the participating component types:
# as long as we communicate within threads, Queue.Queue can be used.  If
# processes on the same host are involved, we switch to multiprocessing.Queue.
# For remote component processes we use zero-mq (zmq) queues.  In the cases
# where the setup is undetermined, we'll have to use zmq, too, to cater for all
# options.
#
# To make the queue type switching transparent, we provide a set of queue
# implementations and wrappers, which implement the interface of Queue.Queue:
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
#   join
#
# Our Queue additionally takes 'name', 'role' and 'address' parameter on the
# constructor.  'role' can be 'input', 'bridge' or 'output', where 'input' is
# the end of a queue one can 'put()' messages into, and 'output' the end of the
# queue where one can 'get()' messages from. A 'bridge' acts as as a message
# forwarder.  'address' denominates a connection endpoint, and 'name' is
# a unique identifier: if multiple instances in the current process space use
# the same identifier, they will get the same queue instance.  Those parameters
# are mostly useful for the zmq queue.
#
class _QueueRegistry(object):

    __metaclass__ = ru.Singleton

    def __init__(self):

        # keep mapping between queue names and instances
        self._lock     = mt.RLock()
        self._registry = {QUEUE_THREAD  : dict(),
                          QUEUE_PROCESS : dict()}


    def get(self, flavor, name, ctor):

        if flavor not in QUEUE_TYPES:
            raise ValueError("no such type '%s'" % flavor)

        with self._lock:

            if name in self._registry[flavor]:
                # queue instance for that name exists - return it
              # print 'found queue for %s %s' % (flavor, name)
                return self._registry[flavor][name]

            else:
                # queue does not yet exist: create and register
                queue = ctor()
                self._registry[flavor][name] = queue
              # print 'created queue for %s %s' % (flavor, name)
                return queue

# create a registry instance
_registry = _QueueRegistry()


# ==============================================================================
#
class Queue(object):
    """
    This is really just the queue interface we want to implement
    """
    def __init__(self, flavor, qname, role, address=None):

        self._flavor = flavor
        self._qname  = qname
        self._role   = role
        self._addr   = address
        self._debug  = False
        self._name   = "queue.%s.%s" % (self._qname, self._role)
        self._log    = ru.get_logger('rp.bridges', target="%s.log" % self._name)

        if not self._addr:
            self._addr = 'tcp://*:*'

        if role in [QUEUE_INPUT, QUEUE_OUTPUT]:
            self._log.info("create %s - %s - %s - %s", flavor, qname, role, address)

    @property
    def name(self):
        return self._name

    @property
    def qname(self):
        return self._qname

    @property
    def flavor(self):
        return self._flavor

    @property
    def role(self):
        return self._role

    @property
    def addr(self):
        return self._addr


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Queue.
    #
    @classmethod
    def create(cls, flavor, name, role, address=None):

        # Make sure that we are the base-class!
        if cls != Queue:
            raise TypeError("Queue Factory only available to base class!")

        try:
            impl = {
                QUEUE_THREAD  : QueueThread,
                QUEUE_PROCESS : QueueProcess,
                QUEUE_ZMQ     : QueueZMQ,
            }[flavor]
          # print 'instantiating %s' % impl
            return impl(flavor, name, role, address)
        except KeyError:
            raise RuntimeError("Queue type '%s' unknown!" % flavor)


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
    def put(self, msg):
        raise NotImplementedError('put() is not implemented')


    # --------------------------------------------------------------------------
    #
    def get(self):
        raise NotImplementedError('get() is not implemented')


    # --------------------------------------------------------------------------
    #
    def get_nowait(self):
        raise NotImplementedError('get_nowait() is not implemented')


    # --------------------------------------------------------------------------
    #
    def stop(self):
        raise NotImplementedError('stop() is not implemented')


# ==============================================================================
#
class QueueThread(Queue):

    def __init__(self, flavor, name, role, address=None):

        Queue.__init__(self, flavor, name, role, address)
        self._q = _registry.get(flavor, name, pyq.Queue)


    # --------------------------------------------------------------------------
    #
    def put(self, msg):

        if not self._role == QUEUE_INPUT:
            raise RuntimeError("queue %s (%s) can't put()" % (self._qname, self._role))

        self._q.put(msg)


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._role == QUEUE_OUTPUT:
            raise RuntimeError("queue %s (%s) can't get()" % (self._qname, self._role))

        return self._q.get()


    # --------------------------------------------------------------------------
    #
    def get_nowait(self):

        if not self._role == QUEUE_OUTPUT:
            raise RuntimeError("queue %s (%s) can't get_nowait()" % (self._qname, self._role))

        try:
            return self._q.get_nowait()
        except pyq.Empty:
            return None


# ==============================================================================
#
class QueueProcess(Queue):

    def __init__(self, flavor, name, role, address=None):

        Queue.__init__(self, flavor, name, role, address)
        self._q = _registry.get(flavor, name, mp.Queue)


    # --------------------------------------------------------------------------
    #
    def put(self, msg):

        if not self._role == QUEUE_INPUT:
            raise RuntimeError("queue %s (%s) can't put()" % (self._qname, self._role))

        self._q.put(msg)


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._role == QUEUE_OUTPUT:
            raise RuntimeError("queue %s (%s) can't get()" % (self._qname, self._role))

        return self._q.get()


    # --------------------------------------------------------------------------
    #
    def get_nowait(self):

        if not self._role == QUEUE_OUTPUT:
            raise RuntimeError("queue %s (%s) can't get_nowait()" % (self._qname, self._role))

        try:
            return self._q.get_nowait()
        except pyq.Empty:
            return None


# ==============================================================================
#
class QueueZMQ(Queue):


    def __init__(self, flavor, name, role, address=None):
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
        addresses as obj.bridge_in and obj.bridge_out.
        """

        self._p          = None           # the bridge process
        self._q          = None           # the zmq queue
        self._lock       = mt.RLock()     # for _requested
        self._requested  = False          # send/recv sync
        self._bridge_in  = None           # bridge input  addr
        self._bridge_out = None           # bridge output addr

        Queue.__init__(self, flavor, name, role, address)

        # ----------------------------------------------------------------------
        # behavior depends on the role...
        if self._role == QUEUE_INPUT:
            ctx = zmq.Context()
            self._q = ctx.socket(zmq.PUSH)
            self._q.connect(self._addr)


        # ----------------------------------------------------------------------
        elif self._role == QUEUE_BRIDGE:

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

                    # FIXME: should we cache messages coming in at the pull/push 
                    #        side, so as not to block the push end?

                    ctx = zmq.Context()
                    _in = ctx.socket(zmq.PULL)
                    _in.bind(addr)

                    _out = ctx.socket(zmq.REP)
                    _out.bind(addr)

                    # communicate the bridge ports to the parent process
                    _in_port  =  _in.getsockopt(zmq.LAST_ENDPOINT)
                    _out_port = _out.getsockopt(zmq.LAST_ENDPOINT)

                    pqueue.put([_in_port, _out_port])

                    self._log.info('bound bridge %s to %s : %s', self._name, _in_port, _out_port)

                    # start polling for messages
                    _poll = zmq.Poller()
                    _poll.register(_out, zmq.POLLIN)

                    while True:

                        events = dict(_uninterruptible(_poll.poll, 1000)) # timeout in ms

                        if _out in events:
                            req = _uninterruptible(_out.recv)
                            _uninterruptible(_out.send_json, _uninterruptible(_in.recv_json))

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
        elif self._role == QUEUE_OUTPUT:
            ctx = zmq.Context()
            self._q = ctx.socket(zmq.REQ)
            self._q.connect(self._addr)

        # ----------------------------------------------------------------------
        else:
            raise RuntimeError ("unsupported queue role '%s'" % self._role)


    # --------------------------------------------------------------------------
    #
    def __del__(self):

        self.stop()


    # --------------------------------------------------------------------------
    #
    @property
    def bridge_in(self):
        if self._role != QUEUE_BRIDGE:
            raise TypeError('bridge_in is only defined on a bridge')
        return self._bridge_in


    # --------------------------------------------------------------------------
    #
    @property
    def bridge_out(self):
        if self._role != QUEUE_BRIDGE:
            raise TypeError('bridge_out is only defined on a bridge')
        return self._bridge_out


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
    def put(self, msg):

        if not self._role == QUEUE_INPUT:
            raise RuntimeError("queue %s (%s) can't put()" % (self._qname, self._role))

      # self._log.debug("-> %s", pprint.pformat(msg))
        _uninterruptible(self._q.send_json, msg)


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._role == QUEUE_OUTPUT:
            raise RuntimeError("queue %s (%s) can't get()" % (self._qname, self._role))

        _uninterruptible(self._q.send, 'request')

        msg = _uninterruptible(self._q.recv_json)
      # self._log.debug("<- %s", pprint.pformat(msg))
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
          #     self._log.debug("<< %s", pprint.pformat(msg))
          #     return msg
          #
          # except zmq.Again:
          #     return None

            if _uninterruptible(self._q.poll, flags=zmq.POLLIN, timeout=timeout):
                msg = _uninterruptible(self._q.recv_json)
                self._requested = False
              # self._log.debug("<< %s", pprint.pformat(msg))
                return msg

            else:
                return None


# ------------------------------------------------------------------------------

