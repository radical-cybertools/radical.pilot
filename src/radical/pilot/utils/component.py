
__copyright__ = 'Copyright 2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

# pylint: disable=global-statement   # W0603 global `_components`

import io
import os
import sys
import copy
import time

import threading       as mt
import radical.utils   as ru

from .. import constants as rpc
from .. import states    as rps

from ..messages  import ComponentStartedMessage
from ..messages  import RPCRequestMessage, RPCResultMessage


# ------------------------------------------------------------------------------
#
# ZMQ subscriber threads will not survive a `fork`.  We register all components
# for an at-fork hook to reset the list of subscribers, so that the subscriber
# threads get re-created as needed in the new process.
#
_components = list()


def _atfork_child():
    global _components
    for c in _components:
        c._subscribers = dict()
    _components = list()


ru.atfork(ru.noop, ru.noop, _atfork_child)


# ------------------------------------------------------------------------------
#
class BaseComponent(object):
    '''
    This class provides the basic structure for any RP component which operates
    on stateful things.  It provides means to:

      - define input channels on which to receive new things in certain states
      - define work methods which operate on the things to advance their state
      - define output channels to which to send the things after working on them
      - define notification channels over which messages with other components
        can be exchanged (publish/subscriber channels)

    All low level communication is handled by the base class. Deriving classes
    will register the respective channels, valid state transitions, and work
    methods.  When a 'thing' is received, the component is assumed to have full
    ownership over it, and that no other component will change the 'thing's
    state during that time.

    The main event loop of the component -- `work()` -- is executed on `run()`
    and will not terminate on its own, unless it encounters a fatal error.

    Components inheriting this class should attempt not to use shared resources.
    That will ensure that multiple instances of the component can coexist for
    higher overall system throughput.  Should access to shared resources be
    necessary, it will require some locking mechanism across process boundaries.

    This approach should ensure that

    - 'thing's are always in a well defined state;
    - components are simple and focus on the semantics of 'thing' state
        progression;
    - no state races can occur on 'thing' state progression;
    - only valid state transitions can be enacted (given correct declaration
        of the component's semantics);
    - the overall system is performant and scalable.

    Inheriting classes SHOULD overload the following methods:

    - `initialize()`:
        - set up the component state for operation
        - register input/output/notification channels
        - register work methods
        - register callbacks to be invoked on state notification
        - the component will terminate if this method raises an exception.
    - `work()`
    - called in the main loop of the component process, on all entities
        arriving on input channels.  The component will *not* terminate if this
        method raises an exception.  For termination, `terminate()` must be
        called.
    - `finalize()`
        - tear down the component (close threads, unregister resources, etc).

    Inheriting classes MUST call the constructor::

        class StagingComponent(rpu.BaseComponent):
            def __init__(self, cfg, session):
                super().__init__(cfg, session)

    A component thus must be passed a configuration (either as a path pointing
    to a file name to be opened as `ru.Config`, or as a pre-populated
    `ru.Config` instance).  That config MUST contain a session ID (`sid`) for
    the session under which to run this component, and a uid for the component
    itself which MUST be unique within the scope of the given session.

    Components will send a startup message to the component manager upon
    successful initialization.

    Further, the class must implement the registered work methods, with a
    signature of::

        work(self, things)

    The method is expected to change the state of the 'thing's given.  'Thing's
    will not be pushed to outgoing channels automatically -- to do so, the work
    method has to call (see call documentation for other options)::

        self.advance(thing)

    Until that method is called, the component is considered the sole owner of
    the 'thing's.  After that method is called, the 'thing's are considered
    disowned by the component.  If, however, components return from the work
    methods without calling advance on the given 'thing's, then the component
    keeps ownership of the 'thing's to advance it asynchronously at a later
    point in time.  That implies that a component can collect ownership over an
    arbitrary number of 'thing's over time, and they can be advanced at the
    component's discretion.

    The component process is a stand-alone daemon process which runs outside of
    Python's multiprocessing domain.  As such, it can freely use Python's
    multithreading (and it extensively does so by default) - but developers
    should be aware that spawning additional *processes* in this component is
    discouraged, as Python's process management is not playing well with it's
    multithreading implementation.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):
        '''
        This constructor MUST be called by inheriting classes, as it specifies
        the operation mode of the component: components can spawn a child
        process, or not.

        If a child will be spawned later, then the child process state can be
        initialized by overloading the`initialize()` method.

        Note that this policy should be strictly followed by all derived
        classes, as we will otherwise carry state over the process fork.  That
        can become nasty if the state included any form of locking (like, for
        profiling or locking).

        The symmetric teardown methods are called `finalize()`.

        Constructors of inheriting components *may* call start() in their
        constructor.
        '''

        # register for at-fork hooks
        _components.append(self)

        # NOTE: a fork will not duplicate any threads of the parent process --
        #       but it will duplicate any locks which are shared between the
        #       parent process and its threads -- and those locks might be in
        #       any state at this point.  As such, each child has to make
        #       sure to never, ever, use any of the inherited locks, but instead
        #       to create it's own set of locks in self.initialize.

        self._cfg     = cfg
        self._uid     = self._cfg.uid
        self._sid     = self._cfg.sid
        self._session = session

        # we always need an UID
        assert self._uid, 'Component needs a uid (%s)' % type(self)

        # state we carry over the fork
        self._owner = self._cfg.get('owner', self.uid)
        self._ctype = "%s.%s" % (self.__class__.__module__,
                                 self.__class__.__name__)

        self._reg = self._session._reg

        self._inputs       = dict()       # queues to get things from
        self._outputs      = dict()       # queues to send things to
        self._workers      = dict()       # methods to work on things
        self._publishers   = dict()       # channels to send notifications to
        self._threads      = dict()       # subscriber and idler threads
        self._cb_lock      = mt.RLock()   # guard threaded callback invokations
        self._rpc_lock     = mt.RLock()   # guard threaded rpc calls
        self._rpc_reqs     = dict()       # currently active RPC requests
        self._rpc_handlers = dict()       # RPC handler methods
        self._subscribers  = dict()       # ZMQ Subscriber classes

        if self._owner == self.uid:
            self._owner = 'root'

        self._prof = self._session._get_profiler(name=self.uid)
        self._log  = self._session._get_logger  (name=self.uid,
                                                 level=self._cfg.get('log_lvl'),
                                                 debug=self._cfg.get('debug_lvl'))

        self._q    = None
        self._in   = None
        self._out  = None
        self._poll = None
        self._ctx  = None

        self._thread = None
        self._term   = mt.Event()


    # --------------------------------------------------------------------------
    #
    def start(self):

        # start worker thread
        sync = mt.Event()
        self._thread = mt.Thread(target=self._work_loop, args=[sync])
        self._thread.daemon = True
        self._thread.start()

        while not sync.is_set():

            if not self._thread.is_alive():
                raise RuntimeError('worker thread died during initialization')

            time.sleep(0.01)

        assert self._thread.is_alive()

        # send startup message
        if self._cfg.cmgr_url:
            self._log.debug('send startup message to %s', self._cfg.cmgr_url)
            pipe = ru.zmq.Pipe(mode=ru.zmq.MODE_PUSH, url=self._cfg.cmgr_url)
            pipe.put(ComponentStartedMessage(uid=self.uid, pid=os.getpid()))

        # give the message some time to get out
        time.sleep(0.1)


    # --------------------------------------------------------------------------
    #
    def wait(self):

        while not self._term.is_set():
            time.sleep(0.1)


    # --------------------------------------------------------------------------
    #
    def _work_loop(self, sync):

        self._log.debug('work loop 1')
        try:
            self._initialize()

        except Exception:
            self._log.exception('worker thread initialization failed')
            raise

        self._log.debug('work loop 2')
        sync.set()

        while not self._term.is_set():
            try:
                ret = self.work_cb()
                if not ret:
                    break
            except:
                self._log.exception('work cb error [ignored]')

        try:
            self._finalize()
        except Exception:
            self._log.exception('worker thread finalialization failed')


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def create(cfg, session):

        # TODO:  We keep this static typemap for component startup. The map
        #        should really be derived from rp module inspection via an
        #        `ru.PluginManager`.
        #
        from radical.pilot import pmgr   as rppm
        from radical.pilot import tmgr   as rptm
        from radical.pilot import agent  as rpa

        comp = {
                rpc.PMGR_LAUNCHING_COMPONENT       : rppm.Launching,

                rpc.TMGR_STAGING_INPUT_COMPONENT   : rptm.Input,
                rpc.TMGR_SCHEDULING_COMPONENT      : rptm.Scheduler,
                rpc.TMGR_STAGING_OUTPUT_COMPONENT  : rptm.Output,

                rpc.AGENT_STAGING_INPUT_COMPONENT  : rpa.Input,
                rpc.AGENT_SCHEDULING_COMPONENT     : rpa.Scheduler,
                rpc.AGENT_EXECUTING_COMPONENT      : rpa.Executing,
                rpc.AGENT_STAGING_OUTPUT_COMPONENT : rpa.Output

               }

        assert cfg.kind in comp, '%s not in %s (%s)' % (cfg.kind,
                list(comp.keys()), cfg.uid)

        return comp[cfg.kind].create(cfg, session)


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return "%s <%s> [%s]" % (self.uid, self.__class__.__name__, self._owner)


    # --------------------------------------------------------------------------
    #
    def control_cb(self, topic, msg):
        '''
        This callback can be overloaded by the component to handle any control
        message which is not already handled by the component base class.
        '''

        self._log.debug_4('ctrl msg ignored: %s:%s', topic, msg.get('cmd'))


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):
        '''
        We listen on the control channel for cancel requests, and append any
        found UIDs to our cancel list.  We also listen for RPC requests and
        handle any registered RPC handlers.  All other control messages are
        passed on to the `control_cb` handler which can be overloaded by
        component implementations.
        '''

        # FIXME: We do not check for types of things to cancel - the UIDs are
        #        supposed to be unique.  That abstraction however breaks as we
        #        currently have no abstract 'cancel' command, but instead use
        #        'cancel_tasks'.

        # try to handle message as RPC message
        try:
            msg = ru.zmq.Message.deserialize(msg)

            if isinstance(msg, RPCRequestMessage):

                self._log.debug_4('handle rpc request %s', msg)
                self._handle_rpc_msg(msg)
                return

            elif isinstance(msg, RPCResultMessage):

                if msg.uid in self._rpc_reqs:
                    self._log.debug_4('handle rpc result %s', msg)
                    self._rpc_reqs[msg.uid]['res'] = msg
                    self._rpc_reqs[msg.uid]['evt'].set()

                return

            else:
                raise ValueError('message type not handled')

        except Exception as e:

            # could not be handled - fall through to legacy handlers
            self._log.debug_6('fall back to legacy msg handlers [%s]', repr(e))


        # handle any other message types
        self._log.debug_5('command incoming: %s', msg)

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd == 'cancel_tasks':

            uids = arg['uids']

            if not isinstance(uids, list):
                uids = [uids]

            self._log.debug('register for cancellation: %s', uids)

            with self._cancel_lock:
                self._cancel_list += uids

            # scheduler and executor handle cancelation directly
            if 'agent.scheduler' in repr(self) or \
               'agent.executing' in repr(self):
                self.control_cb(topic, msg)
                return

        elif cmd == 'terminate':
            self._log.info('got termination command')
            self.stop()

        else:
            self._log.debug_1('command handled by implementation: %s', cmd)
            self.control_cb(topic, msg)


    # --------------------------------------------------------------------------
    #
    def _handle_rpc_msg(self, msg):

        self._log.debug('handle rpc request: %s', msg)

        bakout = sys.stdout
        bakerr = sys.stderr

        strout = None
        strerr = None

        val    = None
        out    = None
        err    = None
        exc    = None

        if msg.cmd not in self._rpc_handlers:
            # this RPC message is *silently* ignored
            self._log.debug('no rpc handler for [%s]', msg.cmd)
            return

        rpc_handler, rpc_addr = self._rpc_handlers[msg.cmd]

        if msg.addr and msg.addr != rpc_addr:
            self._log.debug('ignore rpc handler for [%s] [%s])', msg, rpc_addr)
            return

        try:
            self._log.debug('rpc handler for %s: %s',
                            msg.cmd, self._rpc_handlers[msg.cmd])

            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            val = rpc_handler(*msg.args, **msg.kwargs)
            out = strout.getvalue()
            err = strerr.getvalue()

        except Exception as e:
            self._log.exception('rpc call failed: %s' % (msg))
            val = None
            out = strout.getvalue()
            err = strerr.getvalue()
            exc = (repr(e), '\n'.join(ru.get_exception_trace()))

        finally:
            # restore stdio
            sys.stdout = bakout
            sys.stderr = bakerr

        rpc_res = RPCResultMessage(rpc_req=msg, val=val, out=out, err=err, exc=exc)
        self._log.debug_3('rpc reply: %s', rpc_res)

        self.publish(rpc.CONTROL_PUBSUB, rpc_res)


    # --------------------------------------------------------------------------
    #
    def register_rpc_handler(self, cmd, handler, rpc_addr=None):

        self._rpc_handlers[cmd] = [handler, rpc_addr]


    # --------------------------------------------------------------------------
    #
    def rpc(self, cmd, *args, rpc_addr=None, **kwargs):
        '''Remote procedure call.

        Send am RPC command and arguments to the control pubsub and wait for the
        response.  This is a synchronous operation at this point, and it is not
        thread safe to have multiple concurrent RPC calls.
        '''

        self._log.debug_5('rpc call %s(%s, %s)', cmd, args, kwargs)

        rpc_id  = ru.generate_id('%s.rpc' % self._uid)
        rpc_req = RPCRequestMessage(uid=rpc_id, cmd=cmd, addr=rpc_addr,
                                    args=args, kwargs=kwargs)

        self._rpc_reqs[rpc_id] = {
                'req': rpc_req,
                'res': None,
                'evt': mt.Event(),
                'time': time.time(),
                }
        self.publish(rpc.CONTROL_PUBSUB, rpc_req)

        while True:

            if not self._rpc_reqs[rpc_id]['evt'].wait(timeout=60):
                self._log.debug_4('still waiting for rpc request %s', rpc_id)
                continue

            rpc_res = self._rpc_reqs[rpc_id]['res']
            self._log.debug_4('rpc result: %s', rpc_res)

            del self._rpc_reqs[rpc_id]

            if rpc_res.exc:
                raise RuntimeError('rpc failed: %s' % rpc_res.exc)

            return rpc_res.val


    # --------------------------------------------------------------------------
    #
    @property
    def cfg(self):
        return copy.deepcopy(self._cfg)

    @property
    def session(self):
        return self._session

    @property
    def uid(self):
        return self._uid

    @property
    def ctype(self):
        return self._ctype


    # --------------------------------------------------------------------------
    #
    def _initialize(self):
        '''
        initialization of component base class goes here
        '''

        # components can always publish logs, state updates and control messages
     #  self.register_publisher(rpc.LOG_PUBSUB)
        self.register_publisher(rpc.STATE_PUBSUB)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        # set controller callback to handle cancellation requests and RPCs
        self._cancel_list = list()
        self._cancel_lock = mt.RLock()
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)
        self._log.debug('registered control cb')

        # call component level initialize
        self.initialize()
        self._prof.prof('component_init')


    def initialize(self):
        pass  # can be overloaded


    # --------------------------------------------------------------------------
    #
    def _finalize(self):

        self._log.debug('_finalize()')

        # call component level finalize, before we tear down channels
        self.finalize()

        self._log.debug('%s close prof', self.uid)
        try:
            self._prof.prof('component_final')
            self._prof.flush()
            self._prof.close()
        except Exception:
            pass


    def finalize(self):
        pass  # can be overloaded


    # --------------------------------------------------------------------------
    #
    def stop(self):
        '''
        We need to terminate and join all threads, close all communication
        channels, etc.  But we trust on the correct invocation of the finalizers
        to do all this, and thus here only forward the stop request to the base
        class.
        '''

        #  FIXME: implement timeout, or remove parameter
        #   (pylint W0613 should be removed if changes to timeout are applied)

        self._log.info('stop %s (%s : %s) [%s]', self.uid, os.getpid(),
                       ru.get_thread_name(), ru.get_caller_name())

        self._term.set()


    # --------------------------------------------------------------------------
    #
    def register_input(self, states, queue, cb=None, qname=None, path=None):
        '''
        Using this method, the component can be connected to a queue on which
        things are received to be worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing arrival.

        This method will further associate a thing state with a specific worker
        callback `cb`.
        Upon thing arrival, the thing state will be used to lookup the
        respective worker, and the thing will be handed over.  Workers should
        call self.advance(thing), in order to push the thing toward the next
        component.  If, for some reason, that is not possible before the worker
        returns, the component will retain ownership of the thing, and should
        call advance() asynchronously at a later point in time.

        Worker invocation is synchronous, ie. the main event loop will only
        check for the next thing once the worker method returns.
        '''

        states = ru.as_list(states)
        if not states:
            states = [None]  # worker handles stateless entities

        if cb: cbname = cb.__name__
        else : cbname = 'none'

        name = '%s.%s.%s' % (self.uid, cbname,
                             '_'.join([str(s) for s in states]))

        if name in self._inputs:
            raise ValueError('input %s already registered' % name)

        self._inputs[name] = {'queue'  : self.get_input_ep(queue),
                              'qname'  : qname,
                              'states' : states}

        self._log.debug('registered input %s [%s] [%s]', name, queue, qname)

        # we want exactly one worker associated with a state -- but a worker
        # can be responsible for multiple states

        for state in states:

            self._log.debug('%s register input %s: %s', self.uid, state, name)

            if state in self._workers:
                self._log.warn("%s replaces worker %s (%s)"
                        % (self.uid, self._workers[state], state))
            self._workers[state] = cb

            self._log.debug('registered worker %s [%s]', cbname, state)


    # --------------------------------------------------------------------------
    #
    def unregister_input(self, states, qname, worker):
        '''
        This methods is the inverse to the 'register_input()' method.
        '''

        states = ru.as_list(states)
        if not states:
            states = [None]  # worker handles statless entities


        name = '%s.%s.%s' % (self.uid, worker.__name__,
                             '_'.join([str(s) for s in states]))

        if name not in self._inputs:
            self._log.warn('input %s not registered [%s]', name, qname)
            return

        self._inputs[name]['queue'].stop()
        del self._inputs[name]
        self._log.debug('unregistered input %s [%s]', name, qname)

        for state in states:

            self._log.debug('%s unregister input %s (%s)', self.uid, name, state)

            if state not in self._workers:
                self._log.warn('%s input %s unknown', worker.__name__, state)
                continue

            del self._workers[state]


    # --------------------------------------------------------------------------
    #
    def register_output(self, states, qname):
        '''
        Using this method, the component can be connected to a queue to which
        things are sent after being worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing departure.

        If a state but no output is specified, we assume that the state is
        final, and the thing is then considered 'dropped' on calling advance() on
        it.  The advance() will trigger a state notification though, and then
        mark the drop in the log.  No other component should ever again work on
        such a final thing.  It is the responsibility of the component to make
        sure that the thing is in fact in a final state.
        '''

        states = ru.as_list(states)
        if not states:
            states = [None]  # worker handles stateless entities

        for state in states:

            self._log.debug('%s register output %s:%s', self.uid, state, qname)

            # we want a *unique* output queue for each state.
            if state in self._outputs:
                self._log.warn("%s replaces output for %s : %s -> %s"
                        % (self.uid, state, self._outputs[state], qname))

            if not qname:
                # this indicates a final state
                self._log.debug('%s register output to None %s', self.uid, state)
                self._outputs[state] = None

            else:
                # non-final state, ie. we want a queue to push to:
                self._outputs[state] = self.get_output_ep(qname)


    # --------------------------------------------------------------------------
    #
    def get_input_ep(self, qname):
        '''
        return an input endpoint
        '''

        cfg = self._reg['bridges'][qname]

        return ru.zmq.Getter(qname, url=cfg['addr_get'])


    # --------------------------------------------------------------------------
    #
    def get_output_ep(self, qname):
        '''
        return an output endpoint
        '''

        cfg = self._reg['bridges'][qname]

        return ru.zmq.Putter(qname, url=cfg['addr_put'])


    # --------------------------------------------------------------------------
    #
    def unregister_output(self, states):
        '''
        this removes any outputs registerd for the given states.
        '''

        states = ru.as_list(states)
        if not states:
            states = [None]  # worker handles stateless entities

        for state in states:

            self._log.debug('TERM : %s unregister output %s', self.uid, state)

            if state not in self._outputs:
                self._log.warn('state %s has no output registered',  state)
              # raise ValueError('state %s has no output registered' % state)
                continue

            del self._outputs[state]
            self._log.debug('unregistered output for %s', state)


    # --------------------------------------------------------------------------
    #
    def output(self, things, state=None):
        '''
        this pushes the given things to the output queue register for the given
        state
        '''

        # NOTE: we do not check if things are actually in the given state
        things = ru.as_list(things)
        if not things:
            # nothing to do
            return

        if state not in self._outputs:
            raise ValueError('state %s has no output registered' % state)

        if self._outputs[state]:
            # the bridge will sort things into bulks, wit bulk size dependig on
            # bridge configuration
            self._outputs[state].put(things)


    # --------------------------------------------------------------------------
    #
    def register_timed_cb(self, cb, cb_data=None, timer=None):
        '''
        Idle callbacks are invoked at regular intervals -- they are guaranteed
        to *not* be called more frequently than 'timer' seconds, no promise is
        made on a minimal call frequency.  The intent for these callbacks is to
        run lightweight work in semi-regular intervals.
        '''

        name = "%s.idler.%s" % (self.uid, cb.__name__)
        self._log.debug('START: %s register idler %s', self.uid, name)

        with self._cb_lock:
            if name in self._threads:
                raise ValueError('cb %s already registered' % cb.__name__)

            if timer is None: timer = 0.0  # NOTE: busy idle loop
            else            : timer = float(timer)

            # create a separate thread per idle cb, and let it be watched by the
            # ru.Process base class
            #
            # ------------------------------------------------------------------
            # NOTE: idle timing is a tricky beast: if we sleep for too long,
            #       then we have to wait that long on stop() for the thread to
            #       get active again and terminate/join.  So we always sleep
            #       just a little, and explicitly check if sufficient time has
            #       passed to activate the callback.
            class Idler(mt.Thread):

                # --------------------------------------------------------------
                def __init__(self, name, log, timer, cb, cb_data, cb_lock, term):
                    self._name    = name
                    self._log     = log
                    self._timeout = timer
                    self._cb      = cb
                    self._cb_data = cb_data
                    self._cb_lock = cb_lock
                    self._term    = term
                    self._last    = 0.0

                    super().__init__()
                    self.daemon = True
                    self.start()

                def run(self):
                    try:
                        self._log.debug('start idle thread: %s', self._cb)
                        ret = True
                        while ret:
                            if self._term.is_set():
                                break

                            if self._timeout and \
                               self._timeout > (time.time() - self._last):
                                # not yet
                                time.sleep(0.01)  # FIXME: make configurable
                                continue

                            with self._cb_lock:
                                if self._cb_data is not None:
                                    ret = self._cb(cb_data=self._cb_data)
                                else:
                                    ret = self._cb()
                            if self._timeout:
                                self._last = time.time()
                    except:
                        self._log.exception('idle thread failed: %s', self._cb)
            # ------------------------------------------------------------------

            idler = Idler(name=name, timer=timer, log=self._log,
                          cb=cb, cb_data=cb_data, cb_lock=self._cb_lock,
                          term=self._term)
            self._threads[name] = idler

        self._log.debug('%s registered idler %s', self.uid, name)


    # --------------------------------------------------------------------------
    #
    def unregister_timed_cb(self, cb):
        '''
        This method is reverts the register_timed_cb() above: it
        removes an idler from the component, and will terminate the
        respective thread.
        '''

        name = "%s.idler.%s" % (self.uid, cb.__name__)
        self._log.debug('TERM : %s unregister idler %s', self.uid, name)

        with self._cb_lock:

            if name not in self._threads:
                self._log.warn('timed cb %s is not registered', name)
              # raise ValueError('%s is not registered' % name)
                return

            self._threads[name].stop()  # implies join
            del self._threads[name]

        self._log.debug("TERM : %s unregistered idler %s", self.uid, name)


    # --------------------------------------------------------------------------
    #
    def register_publisher(self, pubsub):
        '''
        Using this method, the component can registered itself to be a publisher
        of notifications on the given pubsub channel.
        '''

        assert pubsub not in self._publishers
        cfg = self._reg['bridges.%s' % pubsub]

        self._publishers[pubsub] = ru.zmq.Publisher(channel=pubsub,
                                                    url=cfg['addr_pub'],
                                                    prof=self._prof)

        self._log.debug('registered publisher for %s', pubsub)


    # --------------------------------------------------------------------------
    #
    def register_subscriber(self, pubsub, cb):
        '''
        This method is complementary to the register_publisher() above: it
        registers a subscription to a pubsub channel.  If a notification
        is received on thag channel, the registered callback will be
        invoked.  The callback MUST have one of the signatures:

          callback(topic, msg)

        where 'topic' is set to the name of the pubsub channel.

        The subscription will be handled in a separate thread, which implies
        that the callback invocation will also happen in that thread.  It is the
        caller's responsibility to ensure thread safety during callback
        invocation.
        '''

        cfg = self._reg['bridges'][pubsub]

        if pubsub not in self._subscribers:
            self._subscribers[pubsub] = ru.zmq.Subscriber(channel=pubsub,
                                                          url=cfg['addr_sub'],
                                                          prof=self._prof)

        self._subscribers[pubsub].subscribe(topic=pubsub, cb=cb,
                                            lock=self._cb_lock)


    # --------------------------------------------------------------------------
    #
    def is_canceled(self, task):
        '''
        check if the given task is listed in the cancel list.  If so, advance it
        as CANCELED and return True - otherwise return False.
        '''

        # FIXME: this can become expensive over time
        #        if the cancel list is never cleaned

        with self._cancel_lock:

            tid = task['uid']

            if tid not in self._cancel_list:
                return False

            if 'state' in task:
                self.advance(task, rps.CANCELED, publish=True, push=False)

            # remove from cancel list
            self._cancel_list.remove(tid)

            return True


    # --------------------------------------------------------------------------
    #
    def work_cb(self):
        '''
        This is the main routine of the component, as it runs in the component
        process.  It will first initialize the component in the process context.
        Then it will attempt to get new things from all input queues
        (round-robin).  For each thing received, it will route that thing to the
        respective worker method.  Once the thing is worked upon, the next
        attempt on getting a thing is up.
        '''

        # if there is nothing to check, idle a bit
        if not self._inputs:
            time.sleep(0.1)
            return True

        # TODO: should a poller over all inputs, or better yet register
        #       a callback

        for name in self._inputs:

            qname  = self._inputs[name]['qname']
            queue  = self._inputs[name]['queue']
            states = self._inputs[name]['states']

            # FIXME: a simple, 1-thing caching mechanism would likely
            #        remove the req/res overhead completely (for any
            #        non-trivial worker).
            things = queue.get_nowait(qname=qname, timeout=200)   # microseconds
          # self._log.debug('work_cb %s: %s %s %d', name, queue.channel,
          #                                         qname, len(things))
            things = ru.as_list(things)

            if not things:
                # next input
                continue

            # the worker target depends on the state of things, so we
            # need to sort the things into buckets by state before
            # pushing them
            buckets = dict()
            for thing in things:
                state = thing.get('state')  # can be stateless

                if state not in buckets:
                    buckets[state] = list()
                buckets[state].append(thing)

            # We now can push bulks of things to the workers

            for state,things in buckets.items():

                assert state in states,        'cannot handle state %s' % state
                assert state in self._workers, 'no worker for state %s' % state

                try:

                    # filter out canceled things
                    if self._cancel_list:
                        things = [x for x in things
                                    if not self.is_canceled(x)]

                  # self._log.debug('== got %d things (%s)', len(things), state)
                  # for thing in things:
                  #     self._log.debug('got %s (%s)', thing['uid'], state)

                    self._workers[state](things)

                except Exception as e:

                    # this is not fatal -- only the 'things' fail, not
                    # the component
                    self._log.exception("work %s failed", self._workers[state])

                    if state:
                        for thing in things:
                            thing['exception']        = repr(e)
                            thing['exception_detail'] = \
                                             '\n'.join(ru.get_exception_trace())

                        self.advance(things, rps.FAILED, publish=True,
                                                         push=False)

        # keep work_cb registered
        return True


    # --------------------------------------------------------------------------
    #
    def advance(self, things, state=None, publish=True, push=False, qname=None,
                              ts=None, fwd=False, prof=True):
        '''
        Things which have been operated upon are pushed down into the queues
        again, only to be picked up by the next component, according to their
        state model.  This method will update the thing state, and push it into
        the output queue registered as target for that state.

         - things:  list of things to advance
         - state:   new state to set for the things
         - publish: determine if state update notifications should be issued
         - push:    determine if things should be pushed to outputs
         - fwd:     determine if notifications are forarded to the ZMQ bridge
         - prof:    determine if state advance creates a profile event
           (publish, and push are always profiled)

        'Things' are expected to be a dictionary, and to have 'state', 'uid' and
        optionally 'type' set.

        If 'thing' contains an '$all' key, the complete dict is published;
        otherwise, *only the state* is published.

        This is evaluated in self.publish.
        '''

        if not things:
            return

        if not ts:
            ts = time.time()

        things = ru.as_list(things)

        # assign state, sort things by state
        buckets = dict()
        for thing in things:

            uid = thing['uid']

            if thing['type'] not in ['task', 'pilot']:
                raise TypeError("thing has unknown type (%s)" % uid)

            if state:
                # state advance done here
                thing['state'] = state

            _state = thing['state']

            if prof:
                self._prof.prof('advance', uid=uid, state=_state, ts=ts)

            if _state not in buckets:
                buckets[_state] = list()
            buckets[_state].append(thing)

        for _state,_things in buckets.items():
            self._log.debug('advance bulk: %s [%s, %s, %s]',
                            len(_things), push, publish, _state)

        # should we publish state information on the state pubsub?
        if publish:

            to_publish = list()

            # If '$all' is set, we update the complete thing_dict.
            # Things in final state are also published in full,
            # otherwise we only send 'uid', 'type' and 'state'.
            for thing in things:

                if '$all' in thing:
                    del thing['$all']
                    to_publish.append(thing)

                elif thing['state'] in rps.FINAL:
                    to_publish.append(thing)

                else:
                    tmp = {'uid'   : thing['uid'],
                           'type'  : thing['type'],
                           'state' : thing['state']}
                    to_publish.append(tmp)

            self.publish(rpc.STATE_PUBSUB, {'cmd': 'update',
                                            'arg': to_publish,
                                            'fwd': fwd})

          # ts = time.time()
          # for thing in things:
          #     self._prof.prof('publish', uid=thing['uid'],
          #                     state=thing['state'], ts=ts)

        # never carry $all and across component boundaries!
        for thing in things:
            if '$all' in thing:
                del thing['$all']

        # should we push things downstream, to the next component
        if push:

            # the push target depends on the state of things, so we need to sort
            # the things into buckets by state before pushing them
            # now we can push the buckets as bulks
            for _state,_things in buckets.items():

              # ts = time.time()
                if _state in rps.FINAL:
                    # things in final state are dropped
                    for thing in _things:
                      # self._log.debug('final %s [%s]', thing['uid'], _state)
                        self._prof.prof('drop', uid=thing['uid'], state=_state,
                                        ts=ts)
                    continue

                if _state not in self._outputs:
                    # unknown target state -- error
                    for thing in _things:
                        self._log.error("lost  %s [%s] : %s", thing['uid'],
                                _state, self._outputs)
                        self._prof.prof('lost', uid=thing['uid'], state=_state,
                                        ts=ts)
                    continue

                if not self._outputs[_state]:
                    # empty output -- drop thing
                    for thing in _things:
                        self._log.error('drop  %s [%s]', thing['uid'], _state)
                        self._prof.prof('drop', uid=thing['uid'], state=_state,
                                        ts=ts)
                    continue

                output = self._outputs[_state]

                # push the thing down the drain
                self._log.debug('put bulk %s: %s: %s', _state, len(_things),
                        output.channel)
                output.put(_things, qname=qname)

              # ts = time.time()
              # for thing in _things:
              #     self._prof.prof('put', uid=thing['uid'], state=_state,
              #                     msg=output.name, ts=ts)


    # --------------------------------------------------------------------------
    #
    def publish(self, pubsub, msg, topic=None):
        '''
        push information into a publication channel
        '''

        if not self._publishers.get(pubsub):
            raise RuntimeError("no msg route for '%s': %s" % (pubsub, msg))

        if not topic:
            topic = pubsub

        self._publishers[pubsub].put(topic, msg)


    # --------------------------------------------------------------------------
    #
    def close(self):

        self._term.set()

        for inp in self._inputs:
            self._inputs[inp]['queue'].stop()

        for sub in self._subscribers:
            self._subscribers[sub].stop()

        self._prof.close()
        self._log.close()


# ------------------------------------------------------------------------------
#
class ClientComponent(BaseComponent):

    # client side state advances are *not* forwarded by default (fwd=False)
    def advance(self, things, state=None, publish=True, push=False, qname=None,
                      ts=None, fwd=False, prof=True):

        things = ru.as_list(things)

        # CANCELED and FAILED are always published, never pushed
        if state in [rps.FAILED, rps.CANCELED]:

            for thing in things:
                thing['target_state'] = state

            publish = True
            push    = False

        super().advance(things=things, state=state, publish=publish, push=push,
                        qname=qname, ts=ts, fwd=fwd, prof=prof)


# ------------------------------------------------------------------------------
#
class AgentComponent(BaseComponent):

    # agent side state advances are forwarded by default (fwd=True)
    def advance(self, things, state=None, publish=True, push=False, qname=None,
                      ts=None, fwd=True, prof=True):

        things = ru.as_list(things)

        # CANCELED and FAILED is handled on the client side
        # FIXME: what if `state==None` and `task['state']` is set instead?
        if state in [rps.FAILED, rps.CANCELED]:

            # final state is handled on client side - hand task over to tmgr
            for thing in things:
                thing['target_state'] = state
                thing['control']      = 'tmgr_pending'
                thing['$all']         = True

              # FIXME: something like this should be done on `stage_on_error`
              # if thing['description'].get('stage_on_error'):
              #     thing['state'] = rps.TMGR_STAGING_OUTPUT_PENDING
              # else:
              #     thing['state'] = state

            publish = True
            push    = False

        super().advance(things=things, state=state, publish=publish, push=push,
                        qname=qname, ts=ts, fwd=fwd, prof=prof)


# ------------------------------------------------------------------------------
