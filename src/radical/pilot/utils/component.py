
import os
import sys
import time
import signal

import threading       as mt
import multiprocessing as mp
import radical.utils   as ru

from ..states    import *

from .prof_utils import Profiler, clone_units, drop_units
from .prof_utils import timestamp as util_timestamp

from .queue      import Queue        as rpu_Queue
from .queue      import QUEUE_ZMQ    as rpu_QUEUE_ZMQ
from .queue      import QUEUE_OUTPUT as rpu_QUEUE_OUTPUT
from .queue      import QUEUE_INPUT  as rpu_QUEUE_INPUT

from .pubsub     import Pubsub       as rpu_Pubsub
from .pubsub     import PUBSUB_ZMQ   as rpu_PUBSUB_ZMQ
from .pubsub     import PUBSUB_PUB   as rpu_PUBSUB_PUB
from .pubsub     import PUBSUB_SUB   as rpu_PUBSUB_SUB

# TODO:
#   - add PENDING states
#   - for notifications, change msg from [topic, unit] to [topic, msg]
#   - components should not need to declare the state publisher?


# ==============================================================================
#
class Component(mp.Process):
    """
    This class provides the basic structure for any RP component which operates
    on stateful units.  It provides means to:

      - define input channels on which to receive new units in certain states
      - define work methods which operate on the units to advance their state
      - define output channels to which to send the units after working on them
      - define notification channels over which messages with other components
        can be exchanged (publish/subscriber channels)

    All low level communication is handled by the base class -- deriving classes
    need only to declare the respective channels, valid state transitions, and
    work methods.  When a unit is received, the component is assumed to have
    full ownership over it, and that no other unit will change the unit state
    during that time.

    The main event loop of the component -- run() -- is executed as a separate
    process.  Components inheriting this class should be fully self sufficient,
    and should specifically attempt not to use shared resources.  That will
    ensure that multiple instances of the component can coexist, for higher
    overall system throughput.  Should access to shared resources be necessary,
    it will require some locking mechanism across process boundaries.

    This approach should ensure that

      - units are always in a well defined state;
      - components are simple and focus on the semantics of unit state
        progression;
      - no state races can occur on unit state progression;
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
      - declare input/output/notification channels
      - declare work methods
      - declare callbacks to be invoked on state notification
      - tear down the same on closing

    Inheriting classes MUST call the constructor:

        class StagingComponent(ComponentBase):
            def __init__(self, args):
                ComponentBase.__init__(self)

    Further, the class must implement the declared work methods, with
    a signature of:

        work(self, unit)

    The method is expected to change the unit state.  Units will not be pushed
    to outgoing channels automatically -- to do so, the work method has to call

        self.advance(unit)

    Until that method is called, the component is considered the sole owner of
    the unit.  After that method is called, the unit is considered disowned by
    the component.  It is the component's responsibility to call that method
    exactly once per unit.

    Having said that, components can return from the work methods without
    calling advance, for two reasons.

      - the unit may be in a final state, and is dropping out of the system (it
        will never again advance in the state model)
      - the component keeps ownership of the unit to advance it asynchronously
        at a later point in time.

    That implies that a component can collect ownership over an arbitrary number
    of units over time.  Either way, at most one work method instance will ever
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
    def __init__(self, ctype, cfg):
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

        self._ctype         = ctype
        self._cfg           = cfg
        self._debug         = cfg.get('debug', 'DEBUG') # FIXME
        self._agent_name    = cfg['agent_name']
        self._cname         = "%s.%s.%d" % (self._agent_name, self._ctype, cfg.get('number', 0))
        self._childname     = "%s.child" % self._cname
        self._addr_map      = cfg['bridge_addresses']
        self._parent        = os.getpid() # pid of spawning process
        self._inputs        = list()      # queues to get units from
        self._outputs       = dict()      # queues to send units to
        self._publishers    = dict()      # channels to send notifications to
        self._subscribers   = list()      # callbacks for received notifications
        self._workers       = dict()      # where units get worked upon
        self._idlers        = list()      # idle_callback registry
        self._terminate     = mt.Event()  # signal for thread termination
        self._finalized     = False       # finalization guard
        self._is_parent     = None        # guard initialize/initialize_child
        self._exit_on_error = True        # FIXME: make configurable
        self._cb_lock       = mt.Lock()   # guard threaded callback invokations
        self._clone_cb      = None        # allocate resources on cloning units
        self._drop_cb       = None        # free resources on dropping clones

        # use agent_name for one log per agent, cname for one log per agent and component
        log_name = self._cname
        log_tgt  = self._cname + ".log"
        self._log = ru.get_logger(log_name, log_tgt, self._debug)
        self._log.info('creating %s' % self._cname)

        self._prof = Profiler(self._cname)

        # start the main event loop in a separate process.  At that point, the
        # component will basically detach itself from the parent process, and
        # will only maintain a handle to be used for shutdown
        mp.Process.__init__(self, name=self._cname)


    # --------------------------------------------------------------------------
    #
    @property
    def ctype(self):
        return self._ctype


    # --------------------------------------------------------------------------
    #
    @property
    def cname(self):
        return self._cname


    # --------------------------------------------------------------------------
    #
    @property
    def childname(self):
        return self._childname


    # --------------------------------------------------------------------------
    #
    def initialize(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the parent process, upon start(), and should be used to
        set up component state before units arrive.
        """
        self._log.debug('base initialize (NOOP)')


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the child process, upon start(), and should be used to
        set up component state before units arrive.
        """
        self._log.debug('base initialize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def finalize(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the parent process, upon stop(), and should be used to
        tear down component state after units have been processed.
        """
        self._log.debug('base finalize (NOOP)')


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the child process, upon stop(), and should be used to
        tear down component state after units have been processed.
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

        if self._is_parent != None:
            raise RuntimeError('start() can be called only once')

        self._is_parent = True # run will reset this for the child

        # make sure we don't keep any profile entries buffered across fork
        self._prof.flush()

        # fork child process
        mp.Process.start(self)

        try:
            # this is now the parent process context
            self.initialize()
        except Exception as e:
            self._log.exception ('initialize failed')
            self.stop()
            raise




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

        if self._finalized:
            # only die once
            return

        self._prof.prof("closing")
        self._log.info("closing (%d subscriber threads)" % (len(self._subscribers)))

        # tear down all subscriber threads
        self._terminate.set()
        self_thread = mt.current_thread()
        for t in self._subscribers:
            if t != self_thread:
                self._log.debug('joining  subscriber thread %s' % t)
                t.join()
            else:
                self._log.debug('skipping subscriber thread %s' % t)

        self._log.debug('subscriber threads joined')

        if self._is_parent:
            self._log.info("terminating")
            self._prof.prof("finalize")
            if not self._finalized:
                self._finalized = True
                self.finalize()
                self._prof.prof("finalized")
            else:
                self._prof.prof("not_yet_finalized")

            # Signal the child
            self._log.debug('signalling child')
            self.terminate()

            # Wait for the child process
            self._log.debug('waiting for child')
            self.join()
            self._log.debug('child done')

            self._prof.prof("stopped")
            self._prof.close()

            # If we are called from within a callback, that (means?) we will
            # have skipped one thread for joining above.
            #
            # Note that the thread is *not* joined at this point -- but the
            # parent should not block on shutdown anymore, as the thread is
            # at least gone.
            if self_thread in self._subscribers and self._cb_lock.locked():
                self._log.debug('release subscriber thread %s' % self_thread)
                sys.exit()

        else:
            # we only finalize in the child's main thread.
            # NOTE: this relies on us not to change the name of MainThread
            if self_thread.name == 'MainThread':
                if not self._finalized:
                    self._prof.prof("not_yet_finalized")
                    self._finalized = True
                    self._prof.prof("finalize")
                    self.finalize_child()
                    self._prof.prof("finalized")
                    self._prof.prof("stopped")
                    self._prof.close()
                else:
                    self._prof.prof("already_finalized - ERROR")

            # The child exits here.  If this call happens in a subscriber
            # thread, then it will be caught in the run loop of the main thread,
            # leading to the main thread's demize, which ends up here again...
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
    def declare_input(self, states, input):
        """
        Using this method, the component can be connected to a queue on which
        units are received to be worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon unit arrival.

        For each state specified by an declare_input() call, the component MUST
        also declare an respective work method.
        """

        if not isinstance(states, list):
            states = [states]

        # get address for the queue
        addr = self._addr_map[input]['source']
        self._log.debug("using addr %s for input %s" % (addr, input))

        q = rpu_Queue.create(rpu_QUEUE_ZMQ, input, rpu_QUEUE_OUTPUT, addr)
        self._inputs.append([q, states])

        for state in states:
            self._log.debug('declared input     : %s : %s : %s' \
                    % (state, input, q.name))


    # --------------------------------------------------------------------------
    #
    def declare_output(self, states, output=None):
        """
        Using this method, the component can be connected to a queue to which
        units are sent after being worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon unit departure.

        If a state but no output is specified, we assume that the state is
        final, and the unit is then considered 'dropped' on calling advance() on
        it.  The advance() will trigger a state notification though, and then
        mark the drop in the log.  No other component should ever again work on
        such a final unit.  It is the responsibility of the component to make
        sure that the unit is in fact in a final state.
        """

        if not isinstance(states, list):
            states = [states]

        for state in states:

            # we want a *unique* output queue for each state.
            if state in self._outputs:
                self._log.warn("%s replaces output for %s : %s -> %s" \
                        % (self._cname, state, self._outputs[state], output))

            if not output:
                # this indicates a final state
                self._outputs[state] = None
            else:
                # get address for the queue
                addr = self._addr_map[output]['sink']
                self._log.debug("using addr %s for output %s" % (addr, output))

                # non-final state, ie. we want a queue to push to
                q = rpu_Queue.create(rpu_QUEUE_ZMQ, output, rpu_QUEUE_INPUT, addr)
                self._outputs[state] = q

                self._log.debug('declared output    : %s : %s : %s' \
                     % (state, output, q.name))


    # --------------------------------------------------------------------------
    #
    def declare_worker(self, states, worker):
        """
        This method will associate a unit state with a specific worker.  Upon
        unit arrival, the unit state will be used to lookup the respective
        worker, and the unit will be handed over.  Workers should call
        self.advance(unit), in order to push the unit toward the next component.
        If, for some reason, that is not possible before the worker returns, the
        component will retain ownership of the unit, and should call advance()
        asynchronously at a later point in time.

        Worker invocation is synchronous, ie. the main event loop will only
        check for the next unit once the worker method returns.
        """

        if not isinstance(states, list):
            states = [states]

        # we want exactly one worker associated with a state -- but a worker can
        # be responsible for multiple states
        for state in states:
            if state in self._workers:
                self._log.warn("%s replaces worker for %s (%s)" \
                        % (self._cname, state, self._workers[state]))
            self._workers[state] = worker

            self._log.debug('declared worker    : %s : %s' \
                    % (state, worker.__name__))



    # --------------------------------------------------------------------------
    #
    def declare_idle_cb(self, cb, timeout=0.0):

        if None == timeout:
            timeout = 0.0
        timeout = float(timeout)

        self._idlers.append({
            'cb'      : cb,       # call this whenever we are idle
            'last'    : 0.0,      # was never called before
            'timeout' : timeout}) # call no more often than this many seconds

        self._log.debug('declared idler     : %s : %s' % (cb.__name__, timeout))


    # --------------------------------------------------------------------------
    #
    def declare_publisher(self, topic, pubsub):
        """
        Using this method, the compinent can declare certain notification topics
        (where topic is a string).  For each topic, a pub/sub network will be
        used to distribute the notifications to subscribers of that topic.

        The same topic can be sent to multiple channels -- but that is
        considered bad practice, and may trigger an error in later versions.
        """

        if topic not in self._publishers:
            self._publishers[topic] = list()

        # get address for pubsub
        addr = self._addr_map[pubsub]['sink']
        self._log.debug("using addr %s for pubsub %s" % (addr, pubsub))

        q = rpu_Pubsub.create(rpu_PUBSUB_ZMQ, pubsub, rpu_PUBSUB_PUB, addr)
        self._publishers[topic].append(q)

        self._log.debug('declared publisher : %s : %s : %s' \
                % (topic, pubsub, q.name))

        # FIXME: I am not exactly sure why this is 'needed', but declaring
        #        a published as above and then immediately publishing on that
        #        channel seems to sometimes lead to a loss of messages.
        time.sleep(1)


    # --------------------------------------------------------------------------
    #
    def declare_subscriber(self, topic, pubsub, cb):
        """
        This method is complementary to the declare_publisher() above: it
        declares a subscription to a pubsub channel.  If a notification
        with matching topic is received, the registered callback will be
        invoked.  The callback MUST have the signature:

          callback(topic, msg)

        The subscription will be handled in a separate thread, which implies
        that the callback invocation will also happen in that thread.  It is the
        caller's responsibility to ensure thread safety during callback
        invocation.
        """

        # ----------------------------------------------------------------------
        def _subscriber(q, callback):
            try:
                while not self._terminate.is_set():
                    topic, msg = q.get_nowait(1000) # timout in ms
                    if topic and msg:
                        with self._cb_lock:
                            callback (topic=topic, msg=msg)
            except Exception as e:
                self._log.exception("subscriber failed")
                if self._exit_on_error:
                    raise
        # ----------------------------------------------------------------------

        # get address for pubsub
        addr = self._addr_map[pubsub]['source']
        self._log.debug("using addr %s for pubsub %s" % (addr, pubsub))

        # create a pubsub subscriber, and subscribe to the given topic
        q = rpu_Pubsub.create(rpu_PUBSUB_ZMQ, pubsub, rpu_PUBSUB_SUB, addr)
        q.subscribe(topic)

        t = mt.Thread(target=_subscriber, args=[q,cb],
                      name="%s.subscriber" % self.cname)
        t.start()
        self._subscribers.append(t)

        self._log.debug('%s declared subscriber: %s : %s : %s : %s' \
                % (self._cname, topic, pubsub, cb, t.name))

    # --------------------------------------------------------------------------
    #
    def declare_clone_cb(self, clone_cb):
        """
        The clone callback will be invoked whenever a unit is cloned after
        passing this component.  So, whenever the component allocates some
        resources for a unit which would conflict when being cloned, this
        callback allows to perform the required correction (hi scheduler!).
        """
        self._log.debug('declare clone_cb %s (%s)', clone_cb, os.getpid())
        self._clone_cb = clone_cb


    # --------------------------------------------------------------------------
    #
    def declare_drop_cb(self, drop_cb):
        """
        The drop callback will be invoked whenever a unit is dropped after
        passing this component.  So, whenever the component allocates some
        resources for a cloned unit which could otherwise not be reaped anymore,
        because the unit would not pass some reaping state or something, this
        callback allows to perform the required action (hi scheduler!).
        """
        self._log.debug('declare drop_cb %s (%s)', drop_cb, os.getpid())
        self._drop_cb = drop_cb


    # --------------------------------------------------------------------------
    #
    def _subscriber_check_cb(self):

        for t in self._subscribers:
            if not t.isAlive():
                raise RuntimeError('subscriber %s died' % t.name)


    # --------------------------------------------------------------------------
    #
    def run(self):
        """
        This is the main routine of the component, as it runs in the component
        process.  It will first initialize the component in the process context.
        Then it will attempt to get new units from all input queues
        (round-robin).  For each unit received, it will route that unit to the
        respective worker method.  Once the unit is worked upon, the next
        attempt ar getting a unit is up.
        """

        self._is_parent = False
        self._cname     = self.childname
        self._log       = ru.get_logger(self._cname, "%s.log" % self._cname, self._debug)
        self._prof      = Profiler(self._cname)

        # parent can call terminate, which we translate here into sys.exit(),
        # which is then excepted in the run loop below for an orderly shutdown.
        def sigterm_handler(signum, frame):
            sys.exit()
        signal.signal(signal.SIGTERM, sigterm_handler)

        # reset other signal handlers to their default
        signal.signal(signal.SIGINT,  signal.SIG_DFL)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

        # set process name
        try:
            import setproctitle as spt
            spt.setproctitle('radical.pilot %s' % self._cname)
        except Exception as e:
            pass


        try:
            # configure the component's logger
            log_name  = self._cname
            log_tgt   = self._cname + ".log"
            self._log = ru.get_logger(log_name, log_tgt, self._debug)
            self._log.info('running %s' % self._cname)

            # initialize_child() should declare all input and output channels, and all
            # workers and notification callbacks
            self._prof.prof('initialize')
            self.initialize_child()
            self._prof.prof('initialized')

            # register own idle callback to watch the subscriber threads (1/sec)
            self.declare_idle_cb(self._subscriber_check_cb, 1.0)

            # perform a sanity check: for each declared input state, we expect
            # a corresponding work method to be declared, too.
            for input, states in self._inputs:
                for state in states:
                    if not state in self._workers:
                        raise RuntimeError("%s: no worker declared for input state %s" \
                                        % self._cname, state)


            # The main event loop will repeatedly iterate over all input
            # channels, probing
            while not self._terminate.is_set():

                # if no action occurs in this iteration, invoke idle callbacks
                active = False

                # FIXME: for the default case where we have only one input
                #        channel, we can probably use a more efficient method to
                #        pull for units -- such as blocking recv() (with some
                #        timeout for eventual shutdown)
                # FIXME: a simple, 1-unit caching mechanism would likely remove
                #        the req/res overhead completely (for any non-trivial
                #        worker).
                for input, states in self._inputs:

                    # FIXME: the timeouts have a large effect on throughput, but
                    #        I am not yet sure how best to set them...
                    unit = input.get_nowait(1000) # timeout in microseconds
                    if not unit:
                        continue

                    state = unit['state']
                    uid   = unit['_id']

                    # assert that the unit is in an expected state
                    if state not in states:
                        self.advance(unit, FAILED, publish=True, push=False)
                        self._prof.prof(event='failed', msg="unexpected state %s" % state,
                                uid=unit['_id'], state=unit['state'], logger=self._log.error)
                        continue

                    # depending on the queue we got the unit from, we can either
                    # drop units or clone them to inject new ones
                    unit = drop_units(self._cfg, unit, self.ctype, 'input',
                                      drop_cb=self._drop_cb, logger=self._log)
                    if not unit:
                        self._prof.prof(event='drop', state=state,
                                uid=uid, msg=input.name)
                        continue

                    units = clone_units(self._cfg, unit, self.ctype, 'input',
                                        clone_cb=self._clone_cb, logger=self._log)

                    for _unit in units:

                        uid = _unit['_id']
                        active = True
                        self._prof.prof(event='get', state=state, uid=uid, msg=input.name)


                        # check if we have a suitable worker (this should always be
                        # the case, as per the assertion done before we started the
                        # main loop.  But, hey... :P
                        if not state in self._workers:
                            self._log.error("%s cannot handle state %s: %s" \
                                    % (self._cname, state, _unit))
                            continue

                        # we have an acceptable state and a matching worker -- hand
                        # it over, wait for completion, and then pull for the next
                        # unit
                        try:
                            self._prof.prof(event='work start', state=state, uid=uid)
                            with self._cb_lock:
                                self._workers[state](_unit)
                            self._prof.prof(event='work done ', state=state, uid=uid)

                        except Exception as e:
                            self.advance(_unit, FAILED, publish=True, push=False)
                            self._prof.prof(event='failed', msg=str(e), uid=uid, state=state)
                            self._log.exception("unit %s failed" % uid)

                            if self._exit_on_error:
                                raise


                # if nothing happened, we can call the idle callbacks.  Don't
                # call them more frequently than what they specified as sleep
                # time though!
                if not active:

                    now = time.time()

                    for idler in self._idlers:

                        if (now - idler['last']) > idler['timeout']:

                            try:
                                with self._cb_lock:
                                    if idler['cb']():
                                        # something happened!
                                        idler['last'] = now
                                        active = True
                            except Exception as e:
                                self._log.exception('idle cb failed')
                                if self._exit_on_error:
                                    raise

                if not active:
                    # FIXME: make configurable
                    time.sleep(0.1)


        except Exception as e:
            # We should see that exception only on process termination -- and of
            # course on incorrect implementations of component workers.  We
            # could in principle detect the latter within the loop -- - but
            # since we don't know what to do with the units it operated on, we
            # don't bother...
            self._log.exception('loop exception')

        except SystemExit:
            self._log.debug("Caught exit")

        except:
            # Can be any other signal or interrupt.
            self._log.exception('loop interruption')

        finally:
            # call stop (which calls the finalizers)
            self.stop()


    # --------------------------------------------------------------------------
    #
    def advance(self, units, state=None, publish=True, push=False, prof=True,
                timestamp=None):
        """
        Units which have been operated upon are pushed down into the queues
        again, only to be picked up by the next component, according to their
        state.  This method will update the unit state, and push it into the
        output queue declared as target for that state.

        units:   list of units to advance
        state:   new state to set for the units
        publish: determine if state update notifications should be issued
        push:    determine if units should be pushed to outputs
        prof:    determine if state advance creates a profile event
                 (publish, push, and drop are always profiled)
        """

        if not timestamp:
            timestamp = util_timestamp()

        if not isinstance(units, list):
            units = [units]

        for unit in units:

            uid = unit['_id']

            if state:
                unit['state']          = state
                unit['state_timstamp'] = timestamp
                if prof:
                    self._prof.prof('advance', uid=unit['_id'], state=state,
                            timestamp=timestamp)
            else:
                state = unit['state']

            if publish:
                # send state notifications
                self.publish('state', unit)
                self._prof.prof('publish', uid=unit['_id'], state=unit['state'])

            if push:
                if state not in self._outputs:
                    # unknown target state -- error
                    self._log.error("%s can't route state %s (%s)" \
                            % (self._cname, state, self._outputs.keys()))
                    continue

                if not self._outputs[state]:
                    # empty output -- drop unit
                    self._log.debug('%s %s ===| %s' % ('state', unit['id'], unit['state']))
                    continue


                output = self._outputs[state]

                # depending on the queue we got the unit from, we can now either
                # drop units or clone them to inject new ones
                unit = drop_units(self._cfg, unit, self.ctype, 'output',
                                  drop_cb=self._drop_cb, logger=self._log)
                if not unit:
                    self._prof.prof(event='drop', state=state, uid=uid, msg=output.name)
                    continue

                units = clone_units(self._cfg, unit, self.ctype, 'output',
                                    clone_cb=self._clone_cb, logger=self._log)

                for _unit in units:
                    # FIXME: we should assert that the unit is in a PENDING state.
                    #        Better yet, enact the *_PENDING transition right here...
                    #
                    # push the unit down the drain
                    output.put(_unit)
                    self._prof.prof('put', uid=_unit['_id'], state=state, msg=output.name)


    # --------------------------------------------------------------------------
    #
    def publish(self, topic, msg):
        """
        push information into a publication channel
        """

        if not isinstance(msg, list):
            msg = [msg]

        if topic not in self._publishers:
            self._log.error("%s can't route notification '%s:%s' (%s)" \
                    % (self._cname, topic, msg, self._publishers.keys()))
            return

        if not self._publishers[topic]:
            self._log.error("%s no route for notification '%s:%s' (%s)" \
                    % (self._cname, topic, msg, self._publishers.keys()))
            return

        for m in msg:
            for p in self._publishers[topic]:
                p.put (topic, m)



# ==============================================================================
#
class Worker(Component):
    """
    A Worker is a Component which cannot change the state of the unit it
    handles.  Workers are emplyed as helper classes to mediate between
    components, between components and database, and between components and
    notification channels.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, ctype, cfg):

        Component.__init__(self, ctype, cfg)


    # --------------------------------------------------------------------------
    #
    # we overload state changing methods from component and assert neutrality
    # FIXME: we should insert hooks around callback invocations, too
    #
    def advance(self, units, state=None, publish=True, push=False, prof=True):

        if state:
            raise RuntimeError("worker %s cannot advance state (%s)"
                    % (self.cname, state))

        Component.advance(self, units, state, publish, push, prof)



# ------------------------------------------------------------------------------

