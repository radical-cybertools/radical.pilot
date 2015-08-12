
import os
import sys
import time
import pprint
import signal

import threading       as mt
import multiprocessing as mp
import radical.utils   as ru

from radical.pilot.states import *

from .logger import get_logger   as rpu_get_logger
from .misc   import prof_init    as rpu_prof_init
from .misc   import prof         as rpu_prof

from .queue  import Queue        as rpu_Queue
from .queue  import QUEUE_ZMQ    as rpu_QUEUE_ZMQ
from .queue  import QUEUE_OUTPUT as rpu_QUEUE_OUTPUT
from .queue  import QUEUE_INPUT  as rpu_QUEUE_INPUT

from .pubsub import Pubsub       as rpu_Pubsub
from .pubsub import PUBSUB_ZMQ   as rpu_PUBSUB_ZMQ
from .pubsub import PUBSUB_PUB   as rpu_PUBSUB_PUB
from .pubsub import PUBSUB_SUB   as rpu_PUBSUB_SUB

# TODO:
#   - add profiling
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

    Inheriting classes MUST overload the method:

        initialize(self)

    This method should be used to
      - set up the component state for operation
      - declare input/output/notification channels
      - declare work methods
      - declare callbacks to be invoked on state notification

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
    #  - *_PENDING -> *_QUEUED ?
    #  - make state transitions more formal
    # --------------------------------------------------------------------------

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, logger):
        """
        This constructor MUST be called by inheriting classes.  
        
        Note that it is not executed in the process scope of the main event
        loop -- initialization for the main event loop should be moved to the 
        initialize() method.
        """

        self._cfg         = cfg
        self._log         = logger

        self._name        = cfg.get('name', type(self).__name__)
        self._addr_map    = cfg.get('bridge_addresses', {})
        self._parent      = os.getpid() # pid of spawning process
        self._inputs      = list()      # queues to get units from
        self._outputs     = dict()      # queues to send units to
        self._publishers  = dict()      # channels to send notifications to
        self._subscribers = dict()      # callbacks for received notifications
        self._workers     = dict()      # where units get worked upon
        self._idlers      = list()      # idle_callback registry
        self._threads     = list()      # subscriber threads
        self._terminate   = mt.Event()  # signal for thread termination
        self._debug       = False

        if 'RADICAL_DEBUG' in os.environ:
            self._debug = True

        # start the main event loop in a separate process.  At that point, the
        # component will basically detach itself from the parent process, and
        # will only maintain a handle to be used for shutdown
        mp.Process.__init__(self, name=self._name)


    # --------------------------------------------------------------------------
    #
    def initialize(self):
        """
        This method MUST be overloaded by the components.  It is called *once*
        in the context of the main run(), and should be used to set up component
        state before units arrive
        """
        raise NotImplementedError("initialize() is not implemented by %s" % self._name)


    # --------------------------------------------------------------------------
    #
    def finalize(self):
        """
        This method MAY be overloaded by the components.  It is called *once* in
        the context of the main run(), and shuld be used to tear down component
        states after units have been processed.
        """
        self._log.debug('base finalize (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _finalize(self):
        """
        this is called from the run thread and from the dtor, so that is is
        called from both the parent and child processes, to clean up all
        subscribers etc.
        """

        self._log.info('shut down component %s - %s(%d threads)' \
                % (self._name, os.getpid(), len(self._threads)))

        # tear down all subscriber threads
        self._terminate.set()
        for t in self._threads:
            self._log.debug('joining subscriber thread %s - %s' % (t, os.getpid()))
            t.join()
        self._log.debug('all threads joined')
        self._threads = []


    # --------------------------------------------------------------------------
    #
    def close(self):
        """
        Shut down the process hosting the event loop
        """
        try:
            self.terminate()

        except Exception as e:
            print "error on closing %s: %s" % (self._name, e)


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

        # check if a remote address is configured for the queue
        addr = self._addr_map.get (input)
        self._log.debug("using addr %s for input %s" % (addr, input))

        q = rpu_Queue.create(rpu_QUEUE_ZMQ, input, rpu_QUEUE_OUTPUT, addr)
        self._inputs.append([q, states])


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
                print "WARNING: %s replaces output for %s : %s -> %s" % (self._name, state, self._outputs[state], output)

            if not output:
                # this indicates a final state
                self._outputs[state] = None
            else:
                # check if a remote address is configured for the queue
                addr = self._addr_map.get (output)
                self._log.debug("using addr %s for output %s" % (addr, output))

                # non-final state, ie. we want a queue to push to
                self._outputs[state] = \
                        rpu_Queue.create(rpu_QUEUE_ZMQ, output, rpu_QUEUE_INPUT, addr)


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

        Worker invokation is synchronous, ie. the main event loop will only
        check for the next unit once the worker method returns.
        """

        if not isinstance(states, list):
            states = [states]

        # we want exactly one worker associated with a state -- but a worker can
        # be responsible for multiple states
        for state in states:
            if state in self._workers:
                print "WARNING: %s replaces worker for %s (%s)" % (self._name, state, self._workers[state])
            self._workers[state] = worker


    # --------------------------------------------------------------------------
    #
    def declare_idle_cb(self, cb, timeout=0.0):

        self._idlers.append({
            'cb'      : cb,       # call this whenever we are idle
            'last'    : 0.0,      # was never called before
            'timeout' : timeout}) # call no more often than this many seconds


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

        # check if a remote address is configured for the queue
        addr = self._addr_map.get (pubsub)
        self._log.debug("using addr %s for pubsub %s" % (addr, pubsub))

        q = rpu_Pubsub.create(rpu_PUBSUB_ZMQ, pubsub, rpu_PUBSUB_PUB, addr)
        self._publishers[topic].append(q)


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
        that the callback invokation will also happen in that thread.  It is the
        caller's responsibility to ensure thread safety during callback
        invokation.
        """

        # ----------------------------------------------------------------------
        def _subscriber(q, callback):
            while not self._terminate.is_set():
                topic, msg = q.get_nowait(0.1) # FIXME timout
                if topic and msg:
                    callback (topic=topic, msg=msg)
        # ----------------------------------------------------------------------

        # create a pubsub subscriber, and subscribe to the given topic
        self._log.debug('create subscriber: %s - %s' \
                        % (mt.current_thread().name, os.getpid()))
        q = rpu_Pubsub.create(rpu_PUBSUB_ZMQ, pubsub, rpu_PUBSUB_SUB)
        q.subscribe(topic)

        t = mt.Thread (target=_subscriber, args=[q,cb])
        t.start()
        self._threads.append(t)


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

        dh = ru.DebugHelper()

        # configure the component's logger
        target    = "component_%s.log" % self._name
        level     = self._cfg.get('debug', 'INFO')
        self._log = rpu_get_logger(self._name, target, level)

        # registering a sigterm handler will allow us to call an exit when the
        # parent calls terminate -- which is excepted in the loop below, and we
        # can then cleanly call finalize...
        def sigterm_handler(signum, frame):
            sys.exit()
        signal.signal(signal.SIGTERM, sigterm_handler)

        rpu_prof_init(target="component_%s.prof" % self._name)
        rpu_prof('run %s' % self._name)

        # Initialize() should declare all input and output channels, and all
        # workers and notification callbacks
        rpu_prof('initialize')
        self.initialize()

        # perform a sanity check: for each declared input state, we expect
        # a corresponding work method to be declared, too.
        for input, states in self._inputs:
            for state in states:
                if not state in self._workers:
                    raise RuntimeError("%s: no worker declared for input state %s" % self._name, state)

        try:
            # The main event loop will repeatedly iterate over all input
            # channels, probing 
            while True:
       
                # if no ation occurs in this iteration, invoke idle callbacks
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
                    unit = input.get_nowait(0.1) # FIXME: timeout
                    if not unit:
                        continue

                    active = True

                    # assert that the unit is in an expected state
                    state = unit['state']
                    if state not in states:
                        self.advance(unit, FAILED, publish=True, push=False)
                        rpu_prof(etype='failed', msg="unexpected state %s" % state,
                                uid=unit['_id'], logger=self._log.error)
                        continue

                    # check if we have a suitable worker (this should always be
                    # the case, as per the assertion done before we started the
                    # main loop.  But, hey... :P
                    if not state in self._workers:
                        print "ERROR  : %s cannot handle state %s: %s" % (self._name, state, unit)
                        continue

                    # we have an acceptable state and a matching worker -- hand
                    # it over, wait for completion, and then pull for the next
                    # unit
                    try:
                        rpu_prof(etype='work', msg='state %s' % state, uid=unit['_id'])
                        self._workers[state](unit)
                        rpu_prof(etype='work done', msg='state %s' % state, uid=unit['_id'])

                    except Exception as e:
                        self.advance(unit, FAILED, publish=True, push=False)
                        rpu_prof(etype='failed', msg=str(e), uid=unit['_id'],
                                logger=self._log.exception)


                # if nothing happened, we can call the idle callbacks.  Don't
                # call them more frequently than what they specified as sleep
                # time though!
                if not active:

                    now = time.time()

                    for idler in self._idlers:

                        if (now - idler['last']) > idler['timeout']:

                            if idler['cb']():
                                # something happend!
                                idler['last'] = now
                                active = True

                if not active:
                    # FIXME: make configurable
                    time.sleep(0.1)


        except Exception as e:
            # We should see that exception only on process termination -- and of
            # course on incorrect implementations of component workers.  We
            # could in principle detect the latter within the loop -- - but
            # since we don't know what to do with the units it operated on, we
            # don't bother...
            rpu_prof("loop error", msg=str(e), logger=self._log.exception)


        finally:
            # call finalizers
            self._log.debug('_finalize')
            self._finalize()
            self._log.debug('finalize')
            self.finalize()
            self._log.debug('finalize complete')
            rpu_prof("final")


    # --------------------------------------------------------------------------
    #
    def advance(self, units, state=None, publish=True, push=False):
        """
        Units which have been operated upon are pushed down into the queues
        again, only to be picked up by the next component, according to their
        state.  This method will update the unit state, and push it into the
        output queue declared as target for that state.

        units:   list of units to advance
        state:   new state to set for the units
        publish: determine if state update notifications should be issued
        push:    determine if units should be pushed to outputs
        """

        if not isinstance(units, list):
            units = [units]

        for unit in units:

            if state:
                unit['state'] = state
                rpu_prof('state', uid=unit['_id'], msg=state)

            if publish:
                # send state notifications
                self.publish('state', unit)
                rpu_prof('pub', uid=unit['_id'])

            if push:
                state = unit['state']
                if state not in self._outputs:
                    # unknown target state -- error
                    print "ERROR  : %s can't route state %s (%s)" % (self._name, state, self._outputs.keys())
                    continue

                if not self._outputs[state]:
                    # empty output -- drop unit
                    self._log.debug('%s %s ===| %s' % ('state', unit['id'], unit['state']))
                    continue

                # FIXME: we should assert that the unit is in a PENDING state.
                #        Better yet, enact the *_PENDING transition right here...
                #
                # push the unit down the drain
                self._outputs[state].put(unit)
                rpu_prof('put', uid=unit['_id'], msg=self._outputs[state].name)


    # --------------------------------------------------------------------------
    #
    def publish(self, topic, msg):
        """
        push information into a publication channel
        """

        if not isinstance(msg, list):
            msg = [msg]

        if topic not in self._publishers:
            print "ERROR  : %s can't route notification %s (%s)" % (self._name, topic, self._publishers.keys())
            return

        if not self._publishers[topic]:
            print "ERROR  : %s no route for notification %s (%s)" % (self._name, topic, self._publishers.keys())
            returreturn

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
    def __init__(self, cfg, logger):

        Component.__init__(self, cfg, logger)

    # --------------------------------------------------------------------------
    #
    # we overload state changing methods from component and assert neutrality
    # FIXME: we should insert hooks around callback invocations, too
    #
    def advance(self, units, state=None, publish=True, push=False):

        if state:
            raise RuntimeError("worker %s cannot advance state (%s)"
                    % (self.name, state))

        Component.advance(self, units, state, publish, push)



# ------------------------------------------------------------------------------

