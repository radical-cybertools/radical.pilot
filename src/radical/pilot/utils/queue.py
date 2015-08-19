
import os
import zmq
import time
import pprint
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

# some predefined port numbers
_QUEUE_PORTS  = {
        'pilot_launching_queue'      : 'tcp://*:10002',
        'umgr_scheduling_queue'      : 'tcp://*:10004',
        'umgr_staging_input_queue'   : 'tcp://*:10006',
        'agent_staging_input_queue'  : 'tcp://*:10008',
        'agent_scheduling_queue'     : 'tcp://*:10010',
        'agent_executing_queue'      : 'tcp://*:10012',
        'agent_staging_output_queue' : 'tcp://*:10014',
        'umgr_staging_output_queue'  : 'tcp://*:10016',
        'ping_queue'                 : 'tcp://*:20000',
        'pong_queue'                 : 'tcp://*:20002',
    }

# --------------------------------------------------------------------------
#
# the input-to-bridge end of the queue uses a different port than the
# bridge-to-output end...
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

    addr = _QUEUE_PORTS.get(name)

    if not addr:
        raise LookupError("no addr for queue type '%s'" % name)

    if role != QUEUE_BRIDGE:
        u = ru.Url(addr)
        u.host = '127.0.0.1'
        addr = str(u)

    return addr


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
        self._addr   = ru.Url(address)
        self._debug  = False
        self._logfd  = None
        self._name   = "queue.%s.%s.%d" % (self._qname, self._role, os.getpid())

        if 'msg' in os.environ.get('RADICAL_DEBUG', '').lower():
            self._debug = True

        # sanity check on address
        default_addr = ru.Url(_get_addr(qname, role))

        # we replace only empty parts of the addr with default values
        if not self._addr       : self._addr        = default_addr
        if not self._addr.schema: self._addr.schema = default_addr.schema
        if not self._addr.host  : self._addr.host   = default_addr.host
        if not self._addr.port  : self._addr.port   = default_addr.port

        if not self._addr:
            raise RuntimeError("no default address found for '%s'" % self._qname)

        if role in [QUEUE_INPUT, QUEUE_OUTPUT]:
            self._log ("create %s - %s - %s - %s - %d" \
                    % (flavor, qname, role, address, os.getpid()))

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
    def _log(self, msg):

        if self._debug:
            if not self._logfd:
                self._logfd = open("%s.log" % self._name, 'a')
            self._logfd.write("%15.5f: %-30s: %s\n" % (time.time(), self._qname, msg))
            self._logfd.flush()


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
    def close(self):
        raise NotImplementedError('close() is not implemented')


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
        except Queue.Empty:
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
        except Queue.Empty:
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

        All component will specify the same address.  Addresses are of the form
        'tcp://ip-number:port'.  

        Note that the address is both used for binding and connecting -- so if
        any component lives on a remote host, all components need to use
        a publicly visible ip number, ie. not '127.0.0.1/localhost'.  The
        bridge-to-output communication needs to use a different port number than
        the input-to-bridge communication.  To simplify setup, we expect
        a single address to be used for all components, and will auto-increase
        the bridge-output port by one.  All given port numbers should be *even*.

        """
        Queue.__init__(self, flavor, name, role, address)

        self._p         = None           # the bridge process
        self._q         = None           # the zmq queue
        self._ctx       = zmq.Context()  # one zmq context suffices
        self._lock      = mt.RLock()     # for _requested
        self._requested = False          # send/recv sync

        # zmq checks on address
        if  self._addr.path   != ''    or \
            self._addr.schema != 'tcp' :
            raise ValueError("url '%s' cannot be used for zmq queues" % self._addr)

        if self._addr.port:
            if (self._addr.port % 2):
                raise ValueError("port numbers must be even, not '%d'" % self._addr.port)

        if self._role != QUEUE_BRIDGE:
            if self._addr.host == '*':
                self._addr.host = '127.0.0.1'

        self._log('%s/%s uses addr %s' % (self._qname, self._role, self._addr))


        # ----------------------------------------------------------------------
        # behavior depends on the role...
        if self._role == QUEUE_INPUT:
            self._q = self._ctx.socket(zmq.PUSH)
            self._q.connect(str(self._addr))


        # ----------------------------------------------------------------------
        elif self._role == QUEUE_BRIDGE:

            # ------------------------------------------------------------------
            def _bridge(ctx, addr_in, addr_out):

                # FIXME: should we cache messages coming in at the pull/push 
                #        side, so as not to block the push end?

              # self._log ('in  _bridge: %s %s' % (addr_in, addr_out))
                _in = ctx.socket(zmq.PULL)
                _in.bind(addr_in)

              # self._log ('out _bridge: %s %s' % (addr_in, addr_out))
                _out = ctx.socket(zmq.REP)
                _out.bind(addr_out)

                _poll = zmq.Poller()
                _poll.register(_out, zmq.POLLIN)

                while True:

                  # self._log ('run _bridge: %s %s' % (addr_in, addr_out))

                    events = dict(_poll.poll(1000)) # timeout in ms

                    if _out in events:
                        req = _out.recv()
                        _out.send_json(_in.recv_json())
            # ------------------------------------------------------------------

            addr_in  = str(self._addr)
            addr_out = str(_port_inc(self._addr))
            self._p  = mp.Process(target=_bridge, args=[self._ctx, addr_in, addr_out])
            self._p.start()

        # ----------------------------------------------------------------------
        elif self._role == QUEUE_OUTPUT:
            self._q = self._ctx.socket(zmq.REQ)
            self._q.connect(str(_port_inc(self._addr)))

        # ----------------------------------------------------------------------
        else:
            raise RuntimeError ("unsupported queue role '%s'" % self._role)


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
    def put(self, msg):

        if not self._role == QUEUE_INPUT:
            raise RuntimeError("queue %s (%s) can't put()" % (self._qname, self._role))

      # self._log("-> %s" % pprint.pformat(msg))
        self._q.send_json(msg)


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._role == QUEUE_OUTPUT:
            raise RuntimeError("queue %s (%s) can't get()" % (self._qname, self._role))

        self._q.send('request')

        msg = self._q.recv_json()
      # self._log("<- %s" % pprint.pformat(msg))
        return msg


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout=None): # timeout in ms

        if not self._role == QUEUE_OUTPUT:
            raise RuntimeError("queue %s (%s) can't get_nowait()" % (self._qname, self._role))

        with self._lock: # need to protect self._requested

            if not self._requested:
                # we can only send the request once per recieval
                self._q.send('request')
                self._requested = True

          # try:
          #     msg = self._q.recv_json(flags=zmq.NOBLOCK)
          #     self._requested = False
          #     self._log("<< %s" % pprint.pformat(msg))
          #     return msg
          #
          # except zmq.Again:
          #     return None

            if self._q.poll (flags=zmq.POLLIN, timeout=timeout):
                msg = self._q.recv_json()
                self._requested = False
              # self._log("<< %s" % pprint.pformat(msg))
                return msg

            else:
                return None


# ------------------------------------------------------------------------------

