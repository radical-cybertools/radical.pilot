
import os
import sys
import copy
import time
import pprint
import signal

import threading       as mt
import multiprocessing as mp
import radical.utils   as ru

from ..          import constants      as rpc
from ..          import states         as rps

from .misc       import hostip
from .prof_utils import Profiler
from .prof_utils import timestamp      as util_timestamp

from .queue      import Queue          as rpu_Queue
from .queue      import QUEUE_ZMQ      as rpu_QUEUE_ZMQ
from .queue      import QUEUE_OUTPUT   as rpu_QUEUE_OUTPUT
from .queue      import QUEUE_INPUT    as rpu_QUEUE_INPUT
from .queue      import QUEUE_BRIDGE   as rpu_QUEUE_BRIDGE

from .pubsub     import Pubsub         as rpu_Pubsub
from .pubsub     import PUBSUB_ZMQ     as rpu_PUBSUB_ZMQ
from .pubsub     import PUBSUB_PUB     as rpu_PUBSUB_PUB
from .pubsub     import PUBSUB_SUB     as rpu_PUBSUB_SUB
from .pubsub     import PUBSUB_BRIDGE  as rpu_PUBSUB_BRIDGE


# TODO:
#   - add PENDING states
#   - for notifications, change msg from [pubsub, thing] to [pubsub, msg]
#   - components should not need to registered the state publisher?


# ==============================================================================
#
class Component(mp.Process):
    """
    This class provides the basic structure for any RP component which operates
    on stateful things.  It provides means to:

      - define input channels on which to receive new things in certain states
      - define work methods which operate on the things to advance their state
      - define output channels to which to send the things after working on them
      - define notification channels over which messages with other components
        can be exchanged (publish/subscriber channels)

    All low level communication is handled by the base class -- deriving classes
    need only to register the respective channels, valid state transitions, and
    work methods.  When a thing is received, the component is assumed to have
    full ownership over it, and that no other thing will change the thing state
    during that time.

    The main event loop of the component -- run() -- is executed as a separate
    process.  Components inheriting this class should be fully self sufficient,
    and should specifically attempt not to use shared resources.  That will
    ensure that multiple instances of the component can coexist, for higher
    overall system throughput.  Should access to shared resources be necessary,
    it will require some locking mechanism across process boundaries.

    This approach should ensure that

      - thing are always in a well defined state;
      - components are simple and focus on the semantics of thing state
        progression;
      - no state races can occur on thing state progression;
      - only valid state transitions can be enacted (given correct declaration
        of the component's semantics);
      - the overall system is performant and scalable.

    Inheriting classes may overload the methods:

        initialize
        initialize_child
        finalize
        finalize_child

    These method should be used to
      - set up the component state for operation
      - register input/output/notification channels
      - register work methods
      - register callbacks to be invoked on state notification
      - tear down the same on closing

    Inheriting classes MUST call the constructor:

        class StagingComponent(ComponentBase):
            def __init__(self, args):
                ComponentBase.__init__(self)

    Further, the class must implement the registered work methods, with
    a signature of:

        work(self, thing)

    The method is expected to change the thing state.  Things will not be pushed
    to outgoing channels automatically -- to do so, the work method has to call

        self.advance(thing)

    Until that method is called, the component is considered the sole owner of
    the thing.  After that method is called, the thing is considered disowned by
    the component.  It is the component's responsibility to call that method
    exactly once per thing.

    Having said that, components can return from the work methods without
    calling advance, for two reasons.

      - the thing may be in a final state, and is dropping out of the system (it
        will never again advance in the state model)
      - the component keeps ownership of the thing to advance it asynchronously
        at a later point in time.

    That implies that a component can collect ownership over an arbitrary number
    of things over time.  Either way, at most one work method instance will ever
    be active at any point in time.
    """

    # --------------------------------------------------------------------------
    #
    # FIXME:
    #  - *_PENDING -> * ?
    #  - make state transitions more formal
    # --------------------------------------------------------------------------

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):
        """
        This constructor MUST be called by inheriting classes.

        Note that __init__ is not executed in the process scope of the main
        event loop -- initialization for the main event loop should be moved to
        the initialize_child() method.  Initialization for component input,
        output and callbacks should be done in a separate initialize() method,
        to avoid the situation where __init__ creates threads but later fails
        and main thus ends up without a handle to terminate the threads (__del__
        can deadlock).  initialize() is called during start() in the parent's
        process context.
        """

        self._cfg           = copy.deepcopy(cfg)
        self._session       = session
        self._debug         = cfg.get('debug', 'DEBUG') # FIXME
        self._debug         = 'DEBUG'
        self._name          = cfg.get('name')
        self._ctype         = "%s.%s" % (self.__class__.__module__, 
                                         self.__class__.__name__)
        self._owner         = cfg.get('owner')
                                           # we need owner for alive messages
        self._inputs        = dict()       # queues to get things from
        self._outputs       = dict()       # queues to send things to
        self._publishers    = dict()       # channels to send notifications to
        self._subscribers   = dict()       # callbacks for recv'ed notifications
        self._workers       = dict()       # where things get worked upon
        self._idlers        = dict()       # idle_callback registry
        self._terminate     = mt.Event()   # signal for thread termination
        self._finalized     = False        # finalization guard
        self._is_parent     = True         # guard initialize/initialize_child
        self._cb_lock       = ru.RLock()   # guard threaded callback invokations
        self._heartbeat_on  = False        # heartbeat_cb activation guard
        self._exit_on_error = True         # FIXME: make configurable

        # we always need an UID
        if not hasattr(self, 'uid'):
            raise ValueError('class which inherits Component needs a uid')

        # helper for sub component startup and management
        self._barrier       = mt.Event()  # signal when sub-components are up
        self._barrier_seen  = None        # list of components seen alive
        self._barrier_lock  = ru.RLock()  # lock on barrier_data
        self._components    = list()      # set of sub components started
        self._bridges       = list()      # set of bridges started
        self._addr_map      = dict()      # address map for bridges

        # initialize the Process base class for later fork.  We do that here
        # before we populate any further member vars.
        mp.Process.__init__(self, name=self.uid)

        # get debugging, logging, profiling set up
        self._dh            = ru.DebugHelper(name=self.uid)
        self._log           = ru.get_logger(self.uid, '.', self._debug)
        self._prof          = Profiler(self.uid)

        self._log.info('creating %s', self.uid)

        # all components need at least be able to talk to a control pubsub and
        # to a state pubsub -- but other bridges may be required, too.  We check
        # our config if an address map exists.  If not we check the session.
        # After this, we we check the config for a set of defined bridges.  If
        # that exist, we start them, and add the addresses to out address map.
        # After that we check that map -- if the rpc.CONTROL_PUBSUB or 'state'
        # pubsub is still missing, we give up.
        #
        # NOTE: This implies that we always start bridges in the parent process.

        # check for bridge addresses in the cfg
        self._addr_map = self._cfg.get('bridge_addresses', {})

        # check for bridge addresses in the session
        if not self._addr_map:
            self._addr_map = self._session.cfg.get('bridge_addresses', {})

        # check for bridges to start in the cfg
        bridges = self._cfg.get('bridges', [])
        self.start_bridges(bridges)

        # sanity check on the address map
        if not self._addr_map:
            self._log.debug('bridges: %s', bridges)
            raise RuntimeError('no bridges defined - abort')
        if rpc.CONTROL_PUBSUB not in self._addr_map:
            raise RuntimeError('no control bridge defined - abort')
        if rpc.STATE_PUBSUB not in self._addr_map:
            raise RuntimeError('no state bridge defined - abort')

        self._log.info('addr_map: %s', pprint.pformat(self._addr_map))

        # components can always publish state updates, and send control messages
        # we also subscribe to control messages
        self.register_publisher (rpc.STATE_PUBSUB)
        self.register_publisher (rpc.CONTROL_PUBSUB)
      # self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)

        # Components are designed to be ephemeral: by default they die unless
        # they are kept alive by a parent's heartbeat signal.  That only holds
        # for components which have been start()ed though -- unstarted
        # components (mostly Workers or Managers) require an explicit call to
        # stop() to disappear.
        # The heartbeat monotoring is performed in the parent, which is
        # registering two callbacks:
        #   - an CONTROL_PUBSUB _heartbeat_monitor_cb which listens for 
        #     heartbeats with 'src == self.owner', and records the time of 
        #     heartbeat in self._heartbeat
        #   - an idle _heartbeat_checker_cb which checks the timer in frequent
        #     intervals, and which will call self.stop() if the last heartbeat
        #     is longer that self._heartbeat_timeout seconds ago
        #     # FIXME: make that time configurable or adjustable
        # We already registered publication on the CONTROL_PUBSUB above, so
        # don't need to do that again at this point.
        # 
        # NOTE: The use of the control pubsub for heartbeat messages between all
        #       components may ultimately result in quite so,e traffic, and
        #       importantly in too many (useless) callback invokations. That can
        #       be regulated by the self._heartbeat_interval setting, which sets
        #       the frequency of heartbeat messages sent by the owner.  If that
        #       turns out to be insufficient, we can introduce component
        #       specific topics on the pubsub channel, to filter messages before
        #       callbacks are getting invoked (at the moment, topic is always
        #       the same as pubsub channel name), or of course use a separate
        #       channel...
        #
        # So, in total we register three callbacks: 
        #   - sending heartbeat to sub-components
        #     this only makes sense if we have anybody listening, so we do that
        #     only once components are spawned, and in fact register the idle cb
        #     in start_components.
        #
        #   - receiving heartbeat from owner components
        #   - checking heartbeat timestamp and stop() if heartbeat is missing
        #     heartbeat checks ony make sense if we get heartbeats, ie. if we 
        #     have an owner, so we only start if an owner is defined.  Otherwise
        #     we are considered the root of the component tree (ie. the client
        #     side session, or agent_0).
        #
        # Note that heartbeats are also used to keep sub-agents alive.

        self._heartbeat          = time.time() # time of last heartbeat
        self._heartbeat_interval = 10.0        # frequency of heartbeats
        self._heartbeat_timeout  = 30.0        # die after missing 3 heartbeats

        assert(self._heartbeat_timeout > (2*self._heartbeat_interval))

        if self.owner:
            self.register_idle_cb(self._heartbeat_checker_cb, 
                                  timeout=self._heartbeat_interval)
            self.register_subscriber(rpc.CONTROL_PUBSUB, self._heartbeat_monitor_cb)

        # We keep a static typemap for worker, component and bridge startup. If
        # we ever want to become reeeealy fancy, we can derive that typemap from
        # rp module inspection.
        # NOTE: I'd rather have this as class data than as instance data, but
        #       python stumbles over circular imports at that point :/

        from .. import worker as rpw
        from .. import pmgr   as rppm
        from .. import umgr   as rpum
        from .. import agent  as rpa

        self._typemap = {rpc.UPDATE_WORKER                  : rpw.Update,

                         rpc.PMGR_LAUNCHING_COMPONENT       : rppm.Launching,

                         rpc.UMGR_STAGING_INPUT_COMPONENT   : rpum.Input,
                         rpc.UMGR_SCHEDULING_COMPONENT      : rpum.Scheduler,
                         rpc.UMGR_STAGING_OUTPUT_COMPONENT  : rpum.Output,

                         rpc.AGENT_STAGING_INPUT_COMPONENT  : rpa.Input,
                         rpc.AGENT_SCHEDULING_COMPONENT     : rpa.Scheduler,
                         rpc.AGENT_EXECUTING_COMPONENT      : rpa.Executing,
                         rpc.AGENT_STAGING_OUTPUT_COMPONENT : rpa.Output
                         }


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return "%s <%s> [%s]" % (self.uid, self.__class__.__name__, self._owner)


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):

        pass

        # wait for 'terminate' commands, but only accept those where 'src' is
        # either myself, my session, or my owner

      # cmd = msg['cmd']
      # arg = msg['arg']
      #
      # if cmd == 'shutdown':
      #     src = arg['sender']
      #     if src in [self.uid, self._session.uid, self._owner, None]:
      #       # print '%s called shutdown on me (%s)' % (src, self.uid)
      #         self._log.info('received shutdown command (%s)', arg)
      #         self.stop()


    # --------------------------------------------------------------------------
    #
    def _heartbeat_cb(self):

        # we only send heartbeat if we have anybody listening
        if self._components:
            self._log.debug('heartbeat sent (%s)' % self.uid)
            self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'heartbeat', 
                                              'arg' : { 'sender' : self.uid}})
        else:
            self._log.debug('heartbeat sent skipped (%s)' % self.uid)


    # --------------------------------------------------------------------------
    #
    def _heartbeat_monitor_cb(self, topic, msg):

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'heartbeat':
            sender = arg['sender']
            if sender == self.owner:
                self._log.debug('heartbeat monitored (%s)' % sender)
                self._heartbeat = time.time()
            else:
                self._log.debug('heartbeat ignored (%s)' % sender)


    # --------------------------------------------------------------------------
    #
    def _heartbeat_checker_cb(self):

        if (self._heartbeat + self._heartbeat_timeout) < time.time():
          # print " ### %s heartbeat FAIL (self.owner)" % self.uid
            self._log.debug('heartbeat check failed')
            self.stop()

        else:
          # print " --- %s heartbeat OK" % self.uid
            self._log.debug('heartbeat check ok')

        return False # always sleep


    # --------------------------------------------------------------------------
    #
    @property
    def cfg(self):

        # make sure the config contains all current bridge addresses
        self._cfg['bridge_addresses'] = copy.deepcopy(self._addr_map)

        return copy.deepcopy(self._cfg)


    # --------------------------------------------------------------------------
    #
    @property
    def session(self):
        return self._session


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def owner(self):
        return self._owner


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._name


    # --------------------------------------------------------------------------
    #
    @property
    def ctype(self):
        return self._ctype


    # --------------------------------------------------------------------------
    #
    def _initialize_parent(self):
        """
        parent initialization of component base class goes here
        """
        self._log.debug('base parent initialize')
        self.register_idle_cb(self._thread_watcher_cb, timeout= 1.0)


    # --------------------------------------------------------------------------
    #
    def initialize(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the parent process, upon start(), and should be used to
        set up component state before things arrive.
        """
        self._log.debug('base initialize (NOOP)')


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the child process, upon start(), and should be used to
        set up component state before things arrive.
        """
        self._log.debug('base initialize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def finalize(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the parent process, upon stop(), and should be used to
        tear down component state after things have been processed.
        """
        self._log.debug('base finalize (NOOP)')

        # FIXME: finaliers should unrergister all callbacks/idlers/subscribers


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the child process, upon stop(), and should be used to
        tear down component state after things have been processed.
        """
        self._log.debug('base finalize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def start(self):
        """
        This method will start the child process.  *After* doing so, it will
        call the parent's initialize, so that this is only executed in the
        parent's process context (before fork).  Start will execute the run loop
        in the child process context, and in that context call
        initialize_child() before entering the loop.

        start() essentially performs:

            if fork():
                # parent
                initialize()
            else:
                # child
                initialize_child()
                run()

        """

        # make sure we don't keep any profile entries buffered across fork
        self._prof.flush()

        # fork child process
        mp.Process.start(self)

        try:
            # this is now the parent process context
            self._initialize_parent()
            self.initialize()
        except Exception as e:
            self._log.exception ('initialize failed')
            self.stop()
            raise


    # --------------------------------------------------------------------------
    #
    def no_start(self):
        """
        Some components will actually not start processes -- but we still may
        want to initialize the parent side of things -- for that we have
        'no_start()'.
        """

        try:
            self._initialize_parent()
            self.initialize()
        except Exception as e:
            self._log.exception ('initialize failed')
            self.stop()
            raise


    # --------------------------------------------------------------------------
    #
    def join(self, timeout=None):
        # we only really join when the comoinent child pricess has been started
        if self.pid:
            self._log.debug('%s join   (%s)' % (self.uid, self.pid))
            mp.Process.join(self, timeout)
            self._log.debug('%s joined (%s)' % (self.uid, self.pid))


    # --------------------------------------------------------------------------
    #
    def stop(self):
        """
        Shut down the process hosting the event loop.  If the parent calls
        stop(), the child is simply terminated (no child finalizers are
        called).  If the child calls stop() itself, child finalizers are called
        before calling exit.

        stop() can be called multiple times, and can be called from the
        MainThread, or from sub thread (such as callback invocations) -- but it
        should notes that, if called from a callback, it may not always be able
        to tear down all threads, specifically not the callback thread itself
        and the MainThread.  Safest is calling it once from each the parent's
        and child's MainThread.  Since the finalizers are only called on the
        first invocation to stop(), finalizers can happen in callback threads!

        stop() basically performs:

            tear down all subscriber threads
            if parent:
                finalize()
                self.terminate()
            else:
                finalize_child()
                sys.exit()
        """

        self_thread = mt.current_thread()

        self._log.debug('stop %s (%s: %s) [%s]' % (self.uid, os.getpid(),
                        self.pid, ru.get_caller_name()))

        if self._finalized:
            # this should not happen
            self._log.warning('double close on %s' % self.uid)
            return

        # no matter what happens below, we attempt finalization only once
        self._finalized = True

        # parent and child finalization will have all comoonents and bridges
        # available
        if self._is_parent:
            self._prof.prof("finalize")
            self.finalize()
            self._prof.prof("finalized")
        else:
            self._prof.prof("finalize_child")
            self.finalize_child()
            self._prof.prof("finalized_child")

      # if 'session' in self.uid.lower():
      #     ru.print_stacktrace()

        self._prof.prof("stopping")
        self._log.info("stopping %s"      % self.uid)
        self._log.info("  components  %s" % self._components)
        self._log.info("  bridges     %s" % self._bridges)
        self._log.info("  subscribers %s" % self._subscribers.keys())
        self._log.info("  idlers      %s" % self._idlers.keys())

        # now let everybody know we are about to go away
        self._terminate.set()

        # once terminate is set, we unlock the cb locks -- othewise we'll never
        # be able to join all threads
        self._cb_lock.release()

        # signal all threads to terminate
        for s in self._subscribers:
            self._log.debug('%s -> term %s' % (self.uid, s))
            self._subscribers[s]['term'].set()
        for i in self._idlers:
            self._log.debug('%s -> term %s' % (self.uid, i))
            self._idlers[i]['term'].set()

        # tear down all subscriber threads -- skip current thread
        for s in self._subscribers:
            t = self._subscribers[s]['thread']
            if t != self_thread:
                self._log.debug('%s -> join %s' % (self.uid, s))
                t.join()                                     
                self._log.debug('subscriber thread  joined  %s' % s)

        # tear down all idler threads -- skip current thread
        for i in self._idlers:
            t = self._idlers[i]['thread']
            if t != self_thread:
                self._log.debug('%s -> join %s' % (self.uid, i))
                t.join()                               
                self._log.debug('idler thread  joined  %s' % i)


        # tear down components, workers and bridges
        for component in self._components: 
            self._log.debug('%s -> stop %s' % (self.uid, component))
            component.stop()
        for bridge in self._bridges: 
            self._log.debug('%s -> stop %s' % (self.uid, bridge))
            bridge.stop()

        for component in self._components: 
            self._log.debug('%s -> join %s' % (self.uid, component))
            component.join()
        for bridge in self._bridges: 
            self._log.debug('%s -> join %s' % (self.uid, bridge))
            bridge.join()

        # Signal the child -- if one exists
        if self._is_parent:
            self._log.info("terminating %s" % self.pid)
            try:
                self.terminate()
            except:
                # no problem: child is gone or never existed
                pass

        # If we are called from within a callback, that means we will
        # have skipped one thread for joining above, so we exit it now.
        #
        # Note that the thread is *not* joined at this point -- but the
        # parent should not block on shutdown anymore, as the thread is
        # at least gone.
        if self_thread in [s['thread'] for k,s in self._subscribers.iteritems()]:
            self._log.debug('release subscriber thread %s (exit)' % self_thread)
            sys.exit()
        if self_thread in [i['thread'] for k,i in self._idlers.iteritems()]:
            self._log.debug('release idler thread %s (exit)' % self_thread)
            sys.exit()

        # NOTE: this relies on us not to change the name of MainThread
        if self_thread.name == 'MainThread':
            self._prof.prof("stopped")
            self._prof.close()

        if not self._is_parent:
            # The child exits here.  If this call happens in a subscriber
            # thread, then it will be caught in the run loop of the main thread,
            # leading to the main thread's demize, which ends up here again...
            self._log.debug('child exit after stop')
            sys.exit()


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
    def register_input(self, states, input, worker):
        """
        Using this method, the component can be connected to a queue on which
        things are received to be worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing arrival.

        This method will further associate a thing state with a specific worker.  
        Upon thing arrival, the thing state will be used to lookup the respective
        worker, and the thing will be handed over.  Workers should call
        self.advance(thing), in order to push the thing toward the next component.
        If, for some reason, that is not possible before the worker returns, the
        component will retain ownership of the thing, and should call advance()
        asynchronously at a later point in time.

        Worker invocation is synchronous, ie. the main event loop will only
        check for the next thing once the worker method returns.
        """

        if not isinstance(states, list):
            states = [states]


        name = '%s.%s.%s' % (self.uid, worker.__name__, '_'.join(states))

        if name in self._inputs:
            raise ValueError('input %s already registered' % name)

        # get address for the queue
        addr = self._addr_map[input]['out']
        self._log.debug("using addr %s for input %s" % (addr, input))

        q = rpu_Queue.create(rpu_QUEUE_ZMQ, input, rpu_QUEUE_OUTPUT, addr)
        self._inputs['name'] = {'queue'  : q, 
                                'states' : states}

        self._log.debug('registered input %s', name)

        # we want exactly one worker associated with a state -- but a worker can
        # be responsible for multiple states
        for state in states:

            if state in self._workers:
                self._log.warn("%s replaces worker for %s (%s)" \
                        % (self.uid, state, self._workers[state]))
            self._workers[state] = worker

            self._log.debug('registered worker %s [%s]', worker.__name__, state)


    # --------------------------------------------------------------------------
    #
    def unregister_input(self, states, input, worker):
        """
        This methods is the inverse to the 'register_input()' method.
        """

        if not isinstance(states, list):
            states = [states]

        name = '%s.%s.%s' % (self.uid, worker.__name__, '_'.join(states))

        if name not in self._inputs:
            raise ValueError('input %s not registered' % name)
        del(self._inputs[name])
        self._log.debug('unregistered input %s', name)

        for state in states:
            if state not in self._workers:
                raise ValueError('worker %s not registered for %s' % worker.__name__, state)
            del(self._workers[state])
            self._log.debug('unregistered worker %s [%s]', worker.__name__, state)


    # --------------------------------------------------------------------------
    #
    def register_output(self, states, output=None):
        """
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
        """

        if not isinstance(states, list):
            states = [states]

        for state in states:

            # we want a *unique* output queue for each state.
            if state in self._outputs:
                self._log.warn("%s replaces output for %s : %s -> %s" \
                        % (self.uid, state, self._outputs[state], output))

            if not output:
                # this indicates a final state
                self._outputs[state] = None
            else:
                # get address for the queue
                addr = self._addr_map[output]['in']
                self._log.debug("using addr %s for output %s" % (addr, output))

                # non-final state, ie. we want a queue to push to
                q = rpu_Queue.create(rpu_QUEUE_ZMQ, output, rpu_QUEUE_INPUT, addr)
                self._outputs[state] = q

                self._log.debug('registered output    : %s : %s : %s' \
                     % (state, output, q.name))


    # --------------------------------------------------------------------------
    #
    def unregister_output(self, states):
        """
        this removes any outputs registerd for the given states.
        """

        if not isinstance(states, list):
            states = [states]

        for state in states:
            if not state in self._outputs:
                raise ValueError('state %s hasno output to unregister' % state)
            del(self._outputs[state])
            self._log.debug('unregistered output for %s', name)


    # --------------------------------------------------------------------------
    #
    def register_idle_cb(self, cb, cb_data=None, timeout=None):
        """
        Idle callbacks are invoked at regular intervals from the child's main
        loop.  They are guaranteed to *not* be called more frequently than
        'timeout' seconds, no promise is made on a minimal call frequency.

        The intent for these callbacks is to use idle times, ie. times where no
        actual work is performed in self.work().  For anything else, and
        sepcifically for high throughput concurrency, the component should use
        its own threading.
        """

        name = "%s.idler.%s" % (self.uid, cb.__name__)

        with self._cb_lock:
            if name in self._idlers:
                raise ValueError('cb %s already registered' % cb.__name__)

        if None == timeout:
            timeout = 0.1
        timeout = float(timeout)

        # create a separate thread per idle cb
        # NOTE: idle timing is a tricky beast: if we sleep for too long, then we
        #       have to wait that long on stop() for the thread to get active
        #       again and terminate/join.  So we always sleep for 1sec, and
        #       manually check if timeout has passed before activating the
        #       callback.
        # ----------------------------------------------------------------------
        def _idler(terminate, callback, callback_data, to):
          # print 'thread %10s : %s' % (ru.gettid(), mt.currentThread().name)
            try:
                last = 0.0  # never been called
                while not terminate.is_set():
                    now = time.time()
                    if (now-last) > to:
                        with self._cb_lock:
                            if callback_data != None:
                                callback(cb_data=callback_data)
                            else:
                                callback()
                        last = now
                    time.sleep(0.1)
            except Exception as e:
                self._log.exception("idler failed %s" % mt.currentThread().name)
                if self._exit_on_error:
                    raise
        # ----------------------------------------------------------------------

        # create a idler thread
        e = mt.Event()
        t = mt.Thread(target=_idler, args=[e,cb,cb_data,timeout], name=name)
        t.start()

        with self._cb_lock:
            self._idlers[name] = {'term'   : e,  # termination signal
                                  'thread' : t}  # thread handle

        self._log.debug('%s registered idler %s' % (self.uid, t.name))


    # --------------------------------------------------------------------------
    #
    def unregister_idle_cb(self, pubsub, cb):
        """
        This method is reverts the register_idle_cb() above: it
        removes an idler from the component, and will terminate the
        respective thread.
        """

        name = "%s.idler.%s" % (self.uid, cb.__name__)

        with self._cb_lock:
            if name not in self._idlers:
                raise ValueError('%s is not registered' % name)

            entry = self._idlers[name]
            entry['term'].set()
            entry['thread'].join()
            del(self._idlers[name])

        self._log.debug("unregistered idler %s", name)


    # --------------------------------------------------------------------------
    #
    def register_publisher(self, pubsub):
        """
        Using this method, the component can registered itself to be a publisher 
        of notifications on the given pubsub channel.
        """

        if pubsub in self._publishers:
            raise ValueError('publisher for %s already regisgered' % pubsub)

        # get address for pubsub
        if not pubsub in self._addr_map:
            self._log.error('no addr: %s' % pprint.pformat(self._addr_map))
            raise ValueError('no bridge known for pubsub channel %s' % pubsub)

        addr = self._addr_map[pubsub]['in']
        self._log.debug("using addr %s for pubsub %s" % (addr, pubsub))

        q = rpu_Pubsub.create(rpu_PUBSUB_ZMQ, pubsub, rpu_PUBSUB_PUB, addr)
        self._publishers[pubsub] = q

        self._log.debug('registered publisher : %s : %s' % (pubsub, q.name))


    # --------------------------------------------------------------------------
    #
    def unregister_publisher(self, pubsub):
        """
        This removes the registration of a pubsub channel for publishing.
        """

        if pubsub not in self._publishers:
            raise ValueError('publisher for %s is not registered' % pubsub)

        del(self._publishers[pubsub])
        self._log.debug('unregistered publisher %s', pubsub)


    # --------------------------------------------------------------------------
    #
    def register_subscriber(self, pubsub, cb, cb_data=None):
        """
        This method is complementary to the register_publisher() above: it
        registers a subscription to a pubsub channel.  If a notification
        is received on thag channel, the registered callback will be
        invoked.  The callback MUST have one of the signatures:

          callback(topic, msg)
          callback(topic, msg, cb_data)

        where 'topic' is set to the name of the pubsub channel. 

        The subscription will be handled in a separate thread, which implies
        that the callback invocation will also happen in that thread.  It is the
        caller's responsibility to ensure thread safety during callback
        invocation.
        """

        name = "%s.subscriber.%s" % (self.uid, cb.__name__)
        if name in self._subscribers:
            raise ValueError('cb %s already registered for %s' % (cb.__name__, pubsub))

        # get address for pubsub
        if not pubsub in self._addr_map:
            raise ValueError('no bridge known for pubsub channel %s' % pubsub)

        addr = self._addr_map[pubsub]['out']
        self._log.debug("using addr %s for pubsub %s" % (addr, pubsub))

        # create a pubsub subscriber (the pubsub name doubles as topic)
        q = rpu_Pubsub.create(rpu_PUBSUB_ZMQ, pubsub, rpu_PUBSUB_SUB, addr)
        q.subscribe(pubsub)

        # ----------------------------------------------------------------------
        def _subscriber(q, terminate, callback, callback_data):
          # print 'thread %10s : %s' % (ru.gettid(), mt.currentThread().name)
            try:
                while not terminate.is_set():
                    topic, msg = q.get_nowait(1000) # timout in ms
                    self._log.debug("<= %s: %s" % (callback.__name__, [topic, msg]))
                    if topic and msg:
                        if not isinstance(msg,list):
                            msg = [msg]
                        for m in msg:
                            with self._cb_lock:
                                if callback_data != None:
                                    callback(topic=topic, msg=m, cb_data=callback_data)
                                else:
                                    callback(topic=topic, msg=m)
            except Exception as e:
                self._log.exception("subscriber failed %s" % mt.currentThread().name)
                if self._exit_on_error:
                    raise
        # ----------------------------------------------------------------------
        e = mt.Event()
        t = mt.Thread(target=_subscriber, args=[q,e,cb,cb_data], name=name)
        t.start()

        with self._cb_lock:
            self._subscribers[name] = {'term'   : e,  # termination signal
                                       'thread' : t}  # thread handle

        self._log.debug('%s registered %s subscriber %s' % (self.uid, pubsub, t.name))


    # --------------------------------------------------------------------------
    #
    def unregister_subscriber(self, pubsub, cb):
        """
        This method is reverts the register_subscriber() above: it
        removes a subscription from a pubsub channel, and will terminate the
        respective thread.
        """

        name = "%s.subscriber.%s" % (self.uid, cb.__name__)

        with self._cb_lock:
            if name not in self._subscribers:
                raise ValueError('%s is not subscribed to %s' % (cb.__name__, pubsub))

            entry = self._subscribers[name]
            entry['term'].set()
            entry['thread'].join()
            del(self._subscribers[name])

        self._log.debug("unregistered %s", name)


    # --------------------------------------------------------------------------
    #
    def _thread_watcher_cb(self):

        with self._cb_lock:
            for s in self._subscribers:
                t = self._subscribers[s]['thread']
                if not t.is_alive():
                    self._log.error('subscriber %s died', t.name)
                    if self._exit_on_error:
                        self.stop()

            for i in self._idlers:
                t = self._idlers[i]['thread']
                if not t.is_alive():
                    self._log.error('idler %s died', t.name)
                    if self._exit_on_error:
                        self.stop()


    # --------------------------------------------------------------------------
    #
    def _profile_flush_cb(self):

        self._log.handlers[0].flush()
        self._prof.flush()


    # --------------------------------------------------------------------------
    #
    def run(self):
        """
        This is the main routine of the component, as it runs in the component
        process.  It will first initialize the component in the process context.
        Then it will attempt to get new things from all input queues
        (round-robin).  For each thing received, it will route that thing to the
        respective worker method.  Once the thing is worked upon, the next
        attempt on getting a thing is up.
        """

        # set some child-provate state
        self._is_parent = False
        self._uid       = self._uid + '.child'
        self._dh        = ru.DebugHelper(name=self.uid)

        # the very first action after fork is to release all locks and handle
        # we still may hold
        self._cb_lock.release
        self._barrier_lock.release

        # handles and state we don't want to carry over the fork:
        self._inputs        = dict()      # queues to get things from
        self._outputs       = dict()      # queues to send things to
        self._components    = list()      # sub-components
        self._bridges       = list()      # started bridges
        self._publishers    = dict()      # channels to send notifications to
        self._subscribers   = dict()      # callbacks for received notifications
        self._workers       = dict()      # where things get worked upon
        self._idlers        = dict()      # idle_callback registry


        # parent can call terminate, which we translate here into sys.exit(),
        # which is then excepted in the run loop below for an orderly shutdown.
        def sigterm_handler(signum, frame):
            self._log.debug('child exit after sigterm')
            sys.exit()
        signal.signal(signal.SIGTERM, sigterm_handler)

        # reset other signal handlers to their default
        signal.signal(signal.SIGINT,  signal.SIG_DFL)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

      # def sigalrm_handler(signum, frame): sys.exit()
      # def sigterm_handler(signum, frame): sys.exit()
      # def sigint_handler (signum, frame): sys.exit()
      #
      # signal.signal(signal.SIGALRM, sigalrm_handler)
      # signal.signal(signal.SIGTERM, sigterm_handler)
      # signal.signal(signal.SIGINT,  sigint_handler)

        # set process name
        try:
            import setproctitle as spt
            spt.setproctitle('radical.pilot %s' % self.uid)
        except Exception as e:
            pass


        try:
            # configure the component's logger
            self._log = ru.get_logger(self.uid, self.uid + '.log', self._debug)
            self._log.info('running %s' % self.uid)

            # components can always publissh state updates, and commands
            self.register_publisher(rpc.STATE_PUBSUB)
            self.register_publisher(rpc.CONTROL_PUBSUB)

            # initialize_child() should register all input and output channels, and all
            # workers and notification callbacks
            self._prof.prof('initialize')
            self.initialize_child()
            self._prof.prof('initialized')

            # register own idle callbacks to 
            #  - watch the subscriber and idler threads (1/sec)
            #  - flush profiles to the FS (1/sec)
            self.register_idle_cb(self._thread_watcher_cb, timeout= 1.0)
            self.register_idle_cb(self._profile_flush_cb,  timeout=60.0)

            # perform a sanity check: for each registered input state, we expect
            # a corresponding work method to be registered, too.
            for name in self._inputs:
                input  = self._inputs[name]['queue']
                states = self._inputs[name]['states']
                for state in states:
                    if not state in self._workers:
                        raise RuntimeError("%s: no worker registered for input state %s" \
                                          % self.uid, state)

            # setup is done, child initialization is done -- we are alive!
            # if we have an owner, we send an alive message to it
            if self._owner:
                self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'alive',
                                                  'arg' : {'sender' : self.uid, 
                                                           'owner'  : self.owner}})

            # The main event loop will repeatedly iterate over all input
            # channels, probing
            while not self._terminate.is_set():

                # if no action occurs in this iteration, invoke idle callbacks
                active = False

                # FIXME: for the default case where we have only one input
                #        channel, we can probably use a more efficient method to
                #        pull for things -- such as blocking recv() (with some
                #        timeout for eventual shutdown)
                # FIXME: a simple, 1-thing caching mechanism would likely remove
                #        the req/res overhead completely (for any non-trivial
                #        worker).
                for name in self._inputs:
                    input  = self._inputs[name]['queue']
                    states = self._inputs[name]['states']

                    # FIXME: the timeouts have a large effect on throughput, but
                    #        I am not yet sure how best to set them...
                    things = input.get_nowait(1000) # timeout in microseconds

                    if not things:
                        continue

                    if not isinstance(things, list):
                        things = [things]

                    active = True

                    for thing in things:

                        uid    = thing['uid']
                        ttype  = thing['type']
                        state  = thing['state']

                        self._log.debug('got %s (%s)', ttype, thing)
                        self._prof.prof(event='get', state=state, uid=uid, msg=input.name)

                        # assert that the thing is in an expected state
                        if state not in states:
                            self.advance(thing, rps.FAILED, publish=True, push=False)
                            self._prof.prof(event='failed', 
                                    msg="unexpected state %s" % state,
                                    uid=uid, state=state, logger=self._log.error)
                            continue

                        # check if we have a suitable worker (this should always be
                        # the case, as per the assertion done before we started the
                        # main loop.  But, hey... :P
                        if not state in self._workers:
                            self._log.error("%s cannot handle state %s: %s" \
                                    % (self.uid, state, thing))
                            continue

                        # we have an acceptable state and a matching worker -- hand
                        # it over, wait for completion, and then pull for the next
                        # thing
                        try:
                            self._prof.prof(event='work start', state=state, uid=uid)
                            with self._cb_lock:
                                self._workers[state](thing)
                            self._prof.prof(event='work done ', state=state, uid=uid)

                        except Exception as e:
                            self._log.exception("%s failed" % uid)
                            self.advance(thing, rps.FAILED, publish=True, push=False)
                            self._prof.prof(event='failed', msg=str(e), uid=uid, state=state)

                            if self._exit_on_error:
                                raise

                if not active:
                    # FIXME: make configurable
                    time.sleep(0.1)

            self._log.info('loop termination')

        except Exception as e:
            # We should see that exception only on process termination -- and of
            # course on incorrect implementations of component workers.  We
            # could in principle detect the latter within the loop -- - but
            # since we don't know what to do with the things it operated on, we
            # don't bother...
            self._log.exception('loop exception')

        except SystemExit:
            self._log.exception("loop exit")

        except:
            # Can be any other signal or interrupt.
            self._log.exception('loop interruption')

        finally:
            # call stop (which calls the finalizers)
            self._log.info('loop stops')
            self.stop()


    # --------------------------------------------------------------------------
    #
    def advance(self, things, state=None, publish=True, push=False, 
                timestamp=None):
        """
        Things which have been operated upon are pushed down into the queues
        again, only to be picked up by the next component, according to their
        state model.  This method will update the thing state, and push it into
        the output queue registered as target for that state.

        things:  list of things to advance
        state:   new state to set for the things
        publish: determine if state update notifications should be issued
        push:    determine if things should be pushed to outputs
        prof:    determine if state advance creates a profile event
                 (publish, and push are always profiled)

        'Things' are expected to be a dictionary, and to have 'state', 'uid' and
        optionally 'type' set.

        If 'thing' contains an '$all' key, the complete dict is published;
        otherwise, *only the state* is published.

        This is evaluated in self.publish.
        """

        if not timestamp:
            timestamp = util_timestamp()

        if not isinstance(things, list):
            things = [things]

        for thing in things:

            uid   = thing['uid']
            ttype = thing['type']

            if ttype not in ['unit', 'pilot']:
                raise TypeError("thing has unknown type (%s)" % uid)

            if not state:
                # no state advance
                state = thing['state']
            else:
                # state advance
                thing['state'] = state

            self._prof.prof('advance', uid=uid, state=state, timestamp=timestamp)


            if publish:

                # Things in final state are published in full
                if state in rps.FINAL:
                    thing['$all'] = True

                # is '$all' is set, we update the complete thing_dict.  In all
                # other cases, we only send 'uid', 'type' and 'state'.
                if '$all' in thing:

                    del(thing['$all'])
                    to_publish = thing

                else:
                    to_publish = {'uid'   : thing['uid'  ], 
                                  'type'  : thing['type' ],
                                  'state' : thing['state']}

                self.publish(rpc.STATE_PUBSUB, {'cmd': 'update', 'arg': to_publish})
                self._prof.prof('publish', uid=thing['uid'], state=thing['state'])


            if push:
                
                if state in rps.FINAL:
                    # things in final state are dropped
                    self._log.debug('%s %s ===| %s' % ('push', thing['uid'], thing['state']))
                    continue

                if not self._outputs[state]:
                    # empty output -- drop thing
                    self._log.debug('%s %s ~~~| %s' % ('push', thing['uid'], thing['state']))
                    continue

                if state not in self._outputs:
                    # unknown target state -- error
                    self._log.error("%s can't route state %s (%s)" \
                            % (self.uid, state, self._outputs.keys()))
                    continue

                output = self._outputs[state]

                # push the thing down the drain
                # FIXME: we should assert that the thing is in a PENDING state.
                #        Better yet, enact the *_PENDING transition right here...
                self._log.debug('%s %s ---> %s' % ('push', thing['uid'], thing['state']))
                output.put(thing)
                self._prof.prof('put', uid=thing['uid'], state=state, msg=output.name)


    # --------------------------------------------------------------------------
    #
    def publish(self, pubsub, msg):
        """
        push information into a publication channel
        """

        if not isinstance(msg, list):
            msg = [msg]

        if pubsub not in self._publishers:
            self._log.error("can't route '%s' notification: %s" % (pubsub, msg))
            return

        if not self._publishers[pubsub]:
            self._log.error("no route for '%s' notification: %s" % (pubsub, msg))
            return

        self._publishers[pubsub].put(pubsub, msg)


    # --------------------------------------------------------------------------
    #
    def start_bridges(self, bridges):
        """
        Helper method to start a given list of bridge names.  The type of bridge
        (queue or pubsub) is derived from the name.  The bridge handles are
        stored in self._bridges, the bridge addresses are in self._addr_map.  If
        new components are started via self.start_components, then the address
        map is passed on as part of the component config (unless an address map
        already is on the config).
        """

        self._log.debug('start_bridges: %s', bridges)

        # we start bridges on localhost -- but since components may be running
        # on other hosts / nodes, we'll try to use a public IP address for the
        # bridge address.
        ip_address = hostip()

        ret = dict()
        for b in bridges:

            if b in self._addr_map:
                self._log.debug('bridge %s already exists', b)
                continue

            self._log.info('create bridge %s', b)
            if b.endswith('queue'):
                bridge = rpu_Queue.create(rpu_QUEUE_ZMQ, b, rpu_QUEUE_BRIDGE)

            elif b.endswith('pubsub'):
                bridge = rpu_Pubsub.create(rpu_PUBSUB_ZMQ, b, rpu_PUBSUB_BRIDGE)

            else:
                raise ValueError('unknown bridge type for %s' % b)

            # FIXME: check if bridge is up and running

            self._bridges.append(bridge)

            addr_in  = ru.Url(bridge.bridge_in)
            addr_out = ru.Url(bridge.bridge_out)

            addr_in.host  = ip_address
            addr_out.host = ip_address

            self._addr_map[b] = {'in'  : str(addr_in ),
                                 'out' : str(addr_out)}

            self._log.info('created bridge %s: %s', b, bridge.name)

        self._log.debug('start_bridges done')


    # --------------------------------------------------------------------------
    #
    def _barrier_cb(self, topic, msg):

        # register 'alive' messages.  Whenever an 'alive' message arrives, we
        # check if a subcomponent spawned by us is the origin.  If that is the
        # case, we record the component as 'alive'.  Whenever we see all current
        # components as 'alive', we unlock the barrier.
        
        cmd = msg['cmd']
        arg = msg['arg']

        # only look at alive messages
        if cmd not in ['alive']:
            # nothing to do
            return

        sender = arg['sender']  # this may be the component we wait for
        owner  = arg['owner']   # only our own components are interesting

        # alive messages usually come from child processes
        if sender.endswith('.child'): 
            sender = sender[:-6]

        # only look at messages from our own children
        if not owner == self.uid:
            return

        # now we are sure to have an 'interesting' alive message
        #
        # the first cb invication will stall here until all component start
        # requests are issued, and self._components is filled.
        with self._barrier_lock:

            if sender not in [c.uid for c in self._components]:
                self._log.error('invalid alive msg for %s' % sender)
                return

            # record component only once
            if sender in self._barrier_seen:
                self._log.error('duplicated alive msg for %s' % sender)
                return

            self._barrier_seen.append(sender)

            if len(self._barrier_seen) < len(self._components):
                self._log.debug( 'barrier %s: incomplete (%s)', self.uid, len(self._barrier_seen))
                return

            # all components have been seen as alive by now - lift barrier
            self._barrier.set()

            # FIXME: this callback can now be unregistered


    # --------------------------------------------------------------------------
    #
    def _component_watcher_cb(self):
        """
        we do a poll() on all our bridges, components, and workers and sub-agent,
        to check if they are still alive.  If any goes AWOL, we will begin to
        tear down this agent.
        """

        to_watch = self._components

        self._log.debug('watching %s things' % len(to_watch))
      # self._log.debug('watching %s' % pprint.pformat(to_watch))

        for thing in to_watch:
            state = thing.poll()
            if state == None:
                self._log.debug('%-40s: ok' % thing.name)
            else:
              # print '%s died? - shutting down' % thing.name
                self._log.error('%s died - shutting down' % thing.name)
                self.stop()

        return True # always idle


    # --------------------------------------------------------------------------
    #
    def start_components(self, components=None, owner=None, timeout=None):
        """
        This method expects a 'components' dict in self._cfg, of the form:

          {
            'component_name' : <number>
          }

        where <number> specifies how many instances are to be created for each
        component.  The 'component_name' is translated into a component class
        type based on self._ctypes.  Components will be passed a deep copy of
        self._cfg, where 'owner' is set to self.uid (unless overwritten by the
        'owner' parameter, and 'number' is set to the index for this component
        type.
        
        On startup, components always connect to the command channel, and will
        send an 'alive' message there.  The starting component will register
        a rpc.CONTROL_PUBSUB subscriber to wait for those 'alive' messages, thus
        creating a startup barrier.  If that barrier is not passed after
        'timeout' seconds (default: 60), the startup is considered to have
        failed.

        An idle callback is also created which waches all spawned components.
        If any of the components is detected as 'failed', stop() is called and
        the component terminates with an error.

        On self.stop(), all components and bridges ever spawned by self will be
        stopped, too.

        Note that this method can also create Workers, which are a specific type
        of components.
        """

        self._log.debug('start components: cfg: %s' % pprint.pformat(self._cfg))

        if not owner  : owner   = self.uid 
        if not timeout: timeout = 60

        # to be on the safe side, we start the heartbeat events before we start
        # the components. It is likely they miss the first one, but that should
        # not really matter.
        if not self._heartbeat_on:
            self._heartbeat_on = True
            self.register_idle_cb(self._heartbeat_cb, 
                                  timeout=self._heartbeat_interval)


        # make sure we get called only once
        if self._barrier_seen != None:
            raise RuntimeError('start_component can only be called once')

        # use the barrier lock during component startup, so that the barrier cb
        # does not start checking before we have anything up...
        with self._barrier_lock:

            # initialize data and start barrier, so that no alive messages get lost
            self._barrier.clear()
            self._barrier_seen = list()
            self.register_subscriber(rpc.CONTROL_PUBSUB, self._barrier_cb)

            start = time.time()

            if not components:
                components = self._cfg.get('components', {})

            self._log.debug("start_components: %s" % components)

            if not components:
                self._log.warn('No components to start')
                return

            for ctype,cnum in components.iteritems():

                maptype = self._typemap.get(ctype)
                if not maptype:
                    raise ValueError('unknown component type (%s)' % ctype)

                for i in range(cnum):

                    self._log.info('create component %s [%s]', maptype, cnum)

                    ccfg = copy.deepcopy(self._cfg)
                    ccfg['components'] = dict()  # avoid recursion
                    ccfg['number']     = i
                    ccfg['owner']      = owner

                    comp = maptype.create(ccfg, self._session)
                    comp.start()

                    self._components.append(comp)


        # at this point, the barrier lock will be released, and the barrier cb
        # can start to collect alive messages from the components we just
        # started.

        if not self._barrier.wait(timeout):
            raise RuntimeError('component startup barrier failed (timeout) (%s != %s' \
                    % (self._barrier_seen, [c.uid for c in self._components]))

        # FIXME: barrier_cb can now be unregistered
        self.unregister_subscriber(rpc.CONTROL_PUBSUB, self._barrier_cb)

        # once components are up, we start a watcher callback
        # FIXME: make timeout configurable?
        self.register_idle_cb(self._component_watcher_cb, timeout=1.0)

        self._log.debug("start_components done")



# ==============================================================================
#
class Worker(Component):
    """
    A Worker is a Component which cannot change the state of the thing it
    handles.  Workers are emplyed as helper classes to mediate between
    components, between components and database, and between components and
    notification channels.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session=None):

        Component.__init__(self, cfg=cfg, session=session)


    # --------------------------------------------------------------------------
    #
    # we overload state changing methods from component and assert neutrality
    # FIXME: we should insert hooks around callback invocations, too
    #
    def advance(self, things, state=None, publish=True, push=False, prof=True):

        if state:
            raise RuntimeError("worker %s cannot advance state (%s)"
                    % (self.uid, state))

        Component.advance(self, things, state, publish, push, prof)


# ------------------------------------------------------------------------------

