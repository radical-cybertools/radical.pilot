
import zmq
import Queue           as pyq
import threading       as mt
import multiprocessing as mp
import radical.utils   as ru

# --------------------------------------------------------------------------
# defines for queue roles
QUEUE_SOURCE  = 'queue_source'
QUEUE_BRIDGE  = 'queue_bridge'
QUEUE_TARGET  = 'queue_target'
QUEUE_ROLES   = [QUEUE_SOURCE, QUEUE_BRIDGE, QUEUE_TARGET]

# defines for queue types
QUEUE_THREAD  = 'queue_thread'
QUEUE_PROCESS = 'queue_process'
QUEUE_ZMQ     = 'queue_zmq'
QUEUE_TYPES   = [QUEUE_THREAD, QUEUE_PROCESS, QUEUE_ZMQ]

# some other local defines
_QUEUE_HWM    = 1   # high water mark == buffer size

# some predefined port numbers
_QUEUE_PORTS  = {
        'pilot_update_queue'         : 'tcp://*:10000',
        'pilot_launching_queue'      : 'tcp://*:10002',
        'unit_update_queue'          : 'tcp://*:10004',
        'umgr_scheduling_queue'      : 'tcp://*:10006',
        'umgr_staging_input_queue'   : 'tcp://*:10008',
        'agent_update_queue'         : 'tcp://*:10010',
        'agent_staging_input_queue'  : 'tcp://*:10012',
        'agent_scheduling_queue'     : 'tcp://*:10014',
        'agent_executing_queue'      : 'tcp://*:10016',
        'agent_staging_output_queue' : 'tcp://*:10018',
        'umgr_staging_output_queue'  : 'tcp://*:10020'
    }

# --------------------------------------------------------------------------
#
# the source-bridge end of the queue uses a different port than the
# bridge-target end...
#
def _port_inc(address):
    u = ru.Url(address)
    u.port += 1
  # print " -> %s" % u
    return str(u)


# --------------------------------------------------------------------------
#
# bridges by default bind to all interfaces on a given port, sources and targets
# connect to localhost (127.0.0.1)
# bridge-target end...
#
def _get_addr(name, role):

    addr = _QUEUE_PORTS.get(name)

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
#   - multiple upstream   components put messages onto the same queue (source)
#   - multiple downstream components get messages from the same queue (target)
#   - local order of messages is maintained
#   - message routing is fair: whatever downstream component calls 'get' first
#     will get the next message (bridge)
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
#   put (item)
#   get ()
#
# Not implemented is, at the moment:
#
#   qsize
#   empty
#   full 
#   put (item, block, timeout
#   put_nowait
#   get (block, timeout)
#   get_nowait
#   task_done
#   join
#
# Our Queue additionally takes 'name', 'role' and 'address' parameter on the
# constructor.  'role' can be 'source', 'bridge' or 'target', where 'source' is
# the sending end of a queue, and 'target' the receiving end, and 'bridge' acts
# as as a message forwarder.  'address' denominates a connection endpoint, and
# 'name' is a unique identifier: if multiple instances in the current process
# space use the same identifier, they will get the same queue instance.  Those
# parameters are obviously mostly useful for the zmq queue.
#
class _QueueRegistry(object):
    
    __metaclass__ = ru.Singleton

    def __init__(self):

        # keep mapping between queue names and instances
        self._lock     = mt.RLock()
        self._registry = {QUEUE_THREAD  : dict(),
                          QUEUE_PROCESS : dict()}


    def get(self, qtype, name, ctor):

        if qtype not in QUEUE_TYPES:
            raise ValueError("no such type '%s'" % qtype)

        with self._lock:
            
            if name in self._registry[qtype]:
                # queue instance for that name exists - return it
              # print 'found queue for %s %s' % (qtype, name)
                return self._registry[qtype][name]

            else:
                # queue does not yet exist: create and register
                queue = ctor()
                self._registry[qtype][name] = queue
              # print 'created queue for %s %s' % (qtype, name)
                return queue

# create a registry instance
_registry = _QueueRegistry()


# ==============================================================================
#
class Queue(object):
    """
    This is really just the queue interface we want to implement
    """
    def __init__(self, qtype, name, role, address=None):

        self._qtype = qtype
        self._name  = name
        self._role  = role
        self._addr  = address # this could have been an ru.Url


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Queue.
    #
    @classmethod
    def create(cls, qtype, name, role, address=None):

        # Make sure that we are the base-class!
        if cls != Queue:
            raise TypeError("Queue Factory only available to base class!")

        try:
            impl = {
                QUEUE_THREAD  : QueueThread,
                QUEUE_PROCESS : QueueProcess,
                QUEUE_ZMQ     : QueueZMQ,
            }[qtype]
          # print 'instantiating %s' % impl
            return impl(qtype, name, role, address)
        except KeyError:
            raise RuntimeError("Queue type '%s' unknown!" % qtype)


    # --------------------------------------------------------------------------
    #
    def put(self, item):
        raise NotImplementedError('put() is not implemented')


    # --------------------------------------------------------------------------
    #
    def get(self):
        raise NotImplementedError('get() is not implemented')


    # --------------------------------------------------------------------------
    #
    def close(self):
        raise NotImplementedError('close() is not implemented')


# ==============================================================================
#
class QueueThread(Queue):

    def __init__(self, qtype, name, role, address=None):

        Queue.__init__(self, qtype, name, role, address)
        self._q = _registry.get(qtype, name, pyq.Queue)


    # --------------------------------------------------------------------------
    #
    def put(self, item):

        if not self._role == QUEUE_SOURCE:
            raise RuntimeError("queue %s (%s) can't put()" % (self._name, self._role))

        if not self._q:
            raise RuntimeError('queue %s (%s) is closed'   % (self._name, self._role))

        self._q.put(item)


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._role == QUEUE_TARGET:
            raise RuntimeError("queue %s (%s) can't get()" % (self._name, self._role))

        if not self._q:
            raise RuntimeError('queue %s (%s) is closed'   % (self._name, self._role))

        return self._q.get()


# ==============================================================================
#
class QueueProcess(Queue):

    def __init__(self, qtype, name, role, address=None):

        Queue.__init__(self, qtype, name, role, address)
        self._q = _registry.get(qtype, name, mp.Queue)


    # --------------------------------------------------------------------------
    #
    def put(self, item):

        if not self._role == QUEUE_SOURCE:
            raise RuntimeError("queue %s (%s) can't put()" % (self._name, self._role))

        if not self._q:
            raise RuntimeError('queue %s (%s) is closed'   % (self._name, self._role))

        self._q.put(item)


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._role == QUEUE_TARGET:
            raise RuntimeError("queue %s (%s) can't get()" % (self._name, self._role))

        if not self._q:
            raise RuntimeError('queue %s (%s) is closed'   % (self._name, self._role))

        return self._q.get()


# ==============================================================================
#
class QueueZMQ(Queue):


    def __init__(self, qtype, name, role, address=None):
        """
        This Queue type sets up an zmq channel of this kind:

        source \            / target
                -- bridge -- 
        source /            \ target

        ie. any number of sources can 'zmq.push()' to a bridge (which
        'zmq.pull()'s), and any number of targets can 'zmq.request()' 
        items from the bridge (which 'zmq.response()'s).

        The bridge is the entity which 'bind()'s network interfaces, both source
        and target type endpoints 'connect()' to it.  It is the callees
        responsibility to ensure that only one bridge of a given type exists.

        All component will specify the same address.  Addresses are of the form
        'tcp://ip-number:port'.  
        
        Note that the address is both used for binding and connecting -- so if
        any component lives on a remote host, all components need to use
        a publicly visible ip number, ie. not '127.0.0.1/localhost'.  The
        bridge-to-target communication needs to use a different port number than
        the source-to-bridge communication.  To simplify setup, we expect
        a single address to be used for all components, and will auto-increase
        the bridge-target port by one.  All given port numbers should be *even*.

        """
        Queue.__init__(self, qtype, name, role, address)

        self._p   = None          # the bridge process
        self._ctx = zmq.Context() # one zmq context suffices

        # sanity check on address
        if not self._addr:  # this may break for ru.Url
            self._addr = _get_addr(name, role)

        if not self._addr:
            raise RuntimeError("no default address found for '%s'" % self._name)

        u = ru.Url(self._addr)
        if  u.path   != ''    or \
            u.schema != 'tcp' :
            raise ValueError("url '%s' cannot be used for zmq queues" % u)

        if (u.port % 2):
            raise ValueError("port numbers must be even, not '%d'" % u.port)

        self._addr = str(u)

        # ----------------------------------------------------------------------
        # behavior depends on the role...
        if self._role == QUEUE_SOURCE:
            self._q       = self._ctx.socket(zmq.PUSH)
            self._q.hwm   = _QUEUE_HWM
            self._q.connect(self._addr)

        # ----------------------------------------------------------------------
        elif self._role == QUEUE_BRIDGE:

            # ------------------------------------------------------------------
            def _bridge(ctx, addr_in, addr_out):
              # print 'in _bridge: %s %s' % (addr_in, addr_out)
                _in      = ctx.socket(zmq.PULL)
                _in.hwm  = _QUEUE_HWM
                _in.bind(addr_in)

                _out     = ctx.socket(zmq.REP)
                _out.hwm = _QUEUE_HWM
                _out.bind(addr_out)

                while True:
                    req = _out.recv()
                    msg = _in.recv_json()
                    # print msg
                    _out.send_json(msg)
            # ------------------------------------------------------------------
            
            addr_in  = self._addr
            addr_out = _port_inc(self._addr)
            self._p = mp.Process(target=_bridge, args=[self._ctx, addr_in, addr_out])
            self._p.start()

        # ----------------------------------------------------------------------
        elif self._role == QUEUE_TARGET:
            self._q       = self._ctx.socket(zmq.REQ)
            self._q.hwm   = _QUEUE_HWM
            self._q.connect(_port_inc(self._addr))

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
    def put(self, item):

        if not self._role == QUEUE_SOURCE:
            raise RuntimeError("queue %s (%s) can't put()" % (self._name, self._role))

        if not self._q:
            raise RuntimeError('queue %s (%s) is closed'   % (self._name, self._role))

        self._q.send_json(item)


    # --------------------------------------------------------------------------
    #
    def get(self):

        if not self._role == QUEUE_TARGET:
            raise RuntimeError("queue %s (%s) can't get()" % (self._name, self._role))

        if not self._q:
            raise RuntimeError('queue %s (%s) is closed'   % (self._name, self._role))

        self._q.send('request')
        return self._q.recv_json()


# ------------------------------------------------------------------------------

