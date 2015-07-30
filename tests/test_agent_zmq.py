#!/usr/bin/env python

import os
import zmq
import time
import copy
import Queue
import pprint
import threading           as mt
import multiprocessing     as mp
import radical.utils       as ru
import radical.pilot.utils as rpu

dh = ru.DebugHelper()

POLL_DELAY = 0.001
UNIT_COUNT = 100

# TODO:
#   - add profiling
#   - add PENDING states
#   - for notifications, change msg from [topic, unit] to [topic, msg]
#   - components should not need to declare the state publisher?

# ==============================================================================
#
class ComponentBase(mp.Process):
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
    def __init__(self):
        """
        This constructor MUST be called by inheriting classes.  
        
        Note that it is not executed in the process scope of the main event
        loop -- initialization for the main event loop should be moved to the 
        initialize() method.
        """

        self._name        = type(self).__name__
        self._parent      = os.getpid() # pid of spawning process
        self._inputs      = list()      # queues to get units from
        self._outputs     = dict()      # queues to send units to
        self._publishers  = dict()      # channels to send notifications to
        self._subscribers = dict()      # callbacks for received notifications
        self._workers     = dict()      # where units get worked upon
        self._debug       = False

        if 'RADICAL_DEBUG' in os.environ:
            self._debug = True

        # start the main event loop in a separate process.  At that point, the
        # component will basically detach itself from the parent process, and
        # will only maintain a handle to be used for shutdown
        mp.Process.__init__(self)
        self.start()


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
        This method CAN be overloaded by the components.  It is called *once* in
        the context of the main run(), and shuld be used to tear down component
        states after units have been processed.
        """

        # FIXME: at the moment, finalize is not called, as the event loop is
        #        forcefully killed.
        pass


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
    def _log(self, msg):
        """
        Log messages to a file.  Log files are individually created for each
        component instance.  Note that logging in any methods which are not
        called from the main event lopp will happen in the scope of the main
        process, and thus result in a separat log file (the process ID is part
        of the logfile name).

        NOTE:  This is a very inefficient log method: it will reopen/flush/close
               the log file for every invokation.  Disabeling logging thus leads
               to a significant speedup of the operatin.

        FIXME: this should use proper python logging
        """

        if self._debug:
            with open("component.%s.%d.log" % (self._name, os.getpid()), 'a') as f:
                f.write("%15.5f: %-30s: %s\n" % (time.time(), self._name, msg))
                f.flush()


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

        q = rpu.Queue.create(rpu.QUEUE_ZMQ, input, rpu.QUEUE_OUTPUT)
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
                # non-final state, ie. we want a queue to push to
                self._outputs[state] = \
                        rpu.Queue.create(rpu.QUEUE_ZMQ, output, rpu.QUEUE_INPUT)


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
    def declare_publisher(self, topic, channel):
        """
        Using this method, the compinent can declare certain notification topics
        (where topic is a string).  For each topic, a pub/sub network will be
        used to distribute the notifications to subscribers of that topic.  
        
        The same topic can be sent to multiple channels -- but that is
        considered bad practice, and may trigger an error in later versions.
        """

        if topic not in self._publishers:
            self._publishers[topic] = list()

        q = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, channel, rpu.PUBSUB_PUB)
        self._publishers[topic].append(q)


    # --------------------------------------------------------------------------
    #
    def declare_subscriber(self, topic, channel, cb):
        """
        This method is complementary to the declare_publisher() above: it
        declares a subscription to a notification channel.  If a notification
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

            while True:
                topic, unit = q.get()
                if topic and unit:
                    callback (topic=topic, unit=unit)
        # ----------------------------------------------------------------------

        # create a pubsub subscriber, and subscribe to the given topic
        q = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, channel, rpu.PUBSUB_SUB)
        q.subscribe(topic)

        t = mt.Thread(target=_subscriber, args=[q,cb])
        t.start()
        # FIXME: shutdown: this should got to the finalize.


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

        # Initialize() should declare all input and output channels, and all
        # workers and notification callbacks
        self._log('initialize')
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

                # FIXME: for the default case where we have only one input
                #        channel, we can probably use a more efficient method to
                #        pull for units -- such as blocking recv() (with some
                #        timeout for eventual shutdown)
                #
                # FIXME: a simple, 1-unit caching mechanism would likely remove
                #        the req/res overhead completely (for any non-trivial
                #        worker).
                for input, states in self._inputs:

                    # FIXME: the timeouts have a large effect on throughput, but
                    #        I am not yet sure how best to set them...
                    unit = input.get_nowait(0.1)
                    if not unit:
                        time.sleep (0.01)
                        continue

                    # assert that the unit is in an expected state
                    # FIXME: enact the *_PENDING -> * transition right here...
                    state = unit['state']
                    if state not in states:
                        print "ERROR  : %s did not expect state %s: %s" % (self._name, state, unit)
                        continue

                    # notify unit arrival
                    self.publish('state', unit)

                    # check if we have a suitable worker (this should always be
                    # the case, as per the assertion done before we started the
                    # main loop.  But, hey... :P
                    if not state in self._workers:
                        print "ERROR  : %s cannot handle state %s: %s" % (self._name, state, unit)
                        continue

                    # we have an acceptable state and a matching worker -- hand
                    # it over, wait for completion, and then pull for the next
                    # unit
                    self._workers[state](unit)

        except Exception as e:
            # We should see that exception only on process termination -- and of
            # course on incorrect implementations of component workers.  We
            # could in principle detect the latter within the loop -- - but
            # since we don't know what to do with the units it operated on, we
            # don't bother...
            self._log("end main loop: %s" % e)

        finally:
            # shut the whole thing down...
            self.finalize()


    # --------------------------------------------------------------------------
    #
    def advance(self, units):
        """
        Units which have been operated upon are pushed down into the queues
        again, only to be picked up by the next component, according to their
        state.  This method will inspect the unit state, and push it into the
        output queue declared as target for that state.
        """

        if not isinstance(units, list):
            units = [units]

        for unit in units:
            state = unit['state']

            # send state notifications
            self.publish('state', unit)

            if state not in self._outputs:
                # unknown target state -- error
                print "ERROR  : %s can't route state %s (%s)" % (self._name, state, self._outputs.keys())
                continue

            if not self._outputs[state]:
                # empty output -- drop unit
                self._log('%s %s ===| %s' % ('state', unit['id'], unit['state']))
                continue

            # FIXME: we should assert that the unit is in a PENDING state.
            #        Better yet, enact the *_PENDING transition right here...
            #
            # push the unit down the drain
            self._outputs[state].put(unit)




    # --------------------------------------------------------------------------
    #
    def publish(self, topic, units):
        """
        push information into a publication channel
        """

        if not isinstance(units, list):
            units = [units]

        if topic not in self._publishers:
            print "ERROR  : %s can't route notification %s (%s)" % (self._name, topic, self._publishers.keys())
            return

        if not self._publishers[topic]:
            print "ERROR  : %s no route for notification %s (%s)" % (self._name, topic, self._publishers.keys())
            returreturn

        for unit in units:
            for p in self._publishers[topic]:
                p.put (topic, unit)


# ==============================================================================
#
class Update(ComponentBase):
    """
    This component subscribes for state update messages, and prints them
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        ComponentBase.__init__(self)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self.declare_subscriber('state', 'agent_state_pubsub', self.state_cb)


    # --------------------------------------------------------------------------
    #
    def state_cb(self, topic, unit):

        if self._debug:
            print '%s %s ---> %s' % (topic, unit['id'], unit['state'])


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        self._log('finalize')


# ==============================================================================
#
class StagingInput(ComponentBase):
    """
    This component will perform input staging for units in STAGING_INPUT state,
    and will advance them to SCHEDULING state.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        ComponentBase.__init__(self)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self.declare_input ('STAGING_INPUT', 'agent_staging_input_queue')
        self.declare_worker('STAGING_INPUT', self.work)

        self.declare_output('SCHEDULING', 'agent_scheduling_queue')

        self.declare_publisher('state', 'agent_state_pubsub')


    # --------------------------------------------------------------------------
    #
    def work(self, unit):

        unit['state'] = 'SCHEDULING'
        self.advance(unit)



# ==============================================================================
#
class Scheduler(ComponentBase):
    """
    This component will assign a limited, shared resource (cores) to units it
    receives.  It will do so asynchronously, ie. whenever it receives either
    a new unit, or a notification that a unit finished, ie. the cores it got
    assigned can be freed.  When a unit gets a core assigned, only then it will
    get advanced from SCHEDULING to EXECUTING state.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        ComponentBase.__init__(self)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._cores     = 500        # limited shared resource
        self._wait_pool = list()     # set of units which wait for the resource
        self._wait_lock = mt.RLock() # look on the above set

        self.declare_input ('SCHEDULING', 'agent_scheduling_queue')
        self.declare_worker('SCHEDULING', self.work_schedule)

        self.declare_output('EXECUTING',  'agent_executing_queue')

        # we need unschedule updates to learn about units which free their
        # allocated cores.  Those updates need to be issued after execution, ie.
        # by the ExecWorker.
        self.declare_publisher ('state',      'agent_state_pubsub')
        self.declare_subscriber('unschedule', 'agent_unschedule_pubsub', self.unschedule_cb)


    # --------------------------------------------------------------------------
    #
    def _alloc(self):
        """
        Check if a core is available for a unit.
        """

        # find a free core
        if self._cores > 0:
            self._cores -= 1
            self._log('---> %d' % self._cores)
            return True
        return False


    # --------------------------------------------------------------------------
    #
    def _dealloc(self):
        """
        Make a formerly assigned core available for new units.
        """

        self._cores += 1
        self._log('===> %d' % self._cores)


    # --------------------------------------------------------------------------
    #
    def work_schedule(self, unit):
        """
        When receiving units, place them in the wait pool and trigger
        a reschedule.
        """

        with self._wait_lock:
            self._wait_pool.append(unit)
        self._reschedule()


    # --------------------------------------------------------------------------
    #
    def unschedule_cb(self, topic, unit):
        """
        This method gets called when a unit frees its allocated core.  We
        deallocate it, and attempt to reschedule() waiting units. 
        """

        if unit['state'] in ['STAGING_OUTPUT', 'DONE', 'FAILED', 'CANCELED']:
            self._dealloc()
            self._reschedule()


    # --------------------------------------------------------------------------
    #
    def _reschedule(self):
        """
        If any resources are available, assign those to waiting units.  Any unit
        which gets a core assigned will be advanced to EXECUTING state, and we
        relinguish control.
        """

        with self._wait_lock:
            while len(self._wait_pool):
               if self._alloc():
                   unit = self._wait_pool[0]
                   self._wait_pool.remove(unit)
                   unit['state'] = 'EXECUTING'
                   # advance unit
                   self.advance(unit)
               else:
                    # don't look further through the wait pool for now
                    break



# ==============================================================================
#
class ExecWorker(ComponentBase):
    """
    This component expectes scheduled units (in EXECUTING state), and will
    execute their workload.  After execution, it will publish an unschedule
    notification.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        ComponentBase.__init__(self)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self.declare_input ('EXECUTING', 'agent_executing_queue')
        self.declare_worker('EXECUTING', self.work)

        self.declare_output('STAGING_OUTPUT', 'agent_staging_output_queue')

        self.declare_publisher('unschedule', 'agent_unschedule_pubsub')
        self.declare_publisher('state',      'agent_state_pubsub')


    # --------------------------------------------------------------------------
    #
    def work(self, unit):
        """
        For each unit we receive, exeute its workload, and then publish an
        unschedule notification.
        """

        # workload
      # time.sleep(1)

        unit['state'] = 'STAGING_OUTPUT'
        self.publish('unschedule', unit)
        self.advance(unit)


# ==============================================================================
#
class StagingOutput(ComponentBase):
    """
    This component will perform output staging for units in STAGING_OUTPUT state,
    and will advance them to the final DONE state.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        ComponentBase.__init__(self)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self.declare_input ('STAGING_OUTPUT', 'agent_staging_output_queue')
        self.declare_worker('STAGING_OUTPUT', self.work)

        # we don't need an output queue -- units are advancing to a final state.
        self.declare_output('DONE', None) # drop final units

        self.declare_publisher('state', 'agent_state_pubsub')


    # --------------------------------------------------------------------------
    #
    def work(self, unit):

        unit['state'] = 'DONE'
        self.advance(unit)



# ==============================================================================
#
def agent():
    """
    This method will instantiate all communitation and notification channels,
    and all components.  It will then feed a set of units to the leading channel
    (staging_input).  A state notification callback will then register all units
    which reached a final state (DONE).  Once all units are accounted for, it
    will tear down all created objects.
    """

    try:

        # keep track of objects we need to close in the finally clause
        bridges    = list()
        components = list()

        # two shortcuts for bridge creation
        def _create_queue_bridge(qname):
            return rpu.Queue.create(rpu.QUEUE_ZMQ, qname, rpu.QUEUE_BRIDGE)

        def _create_pubsub_bridge(channel):
            return rpu.Pubsub.create(rpu.PUBSUB_ZMQ, channel, rpu.PUBSUB_BRIDGE)

        # create all communication bridges we need.  Use the default addresses,
        # ie. they will bind to localhost to ports 10.000++
        bridges.append(_create_queue_bridge('agent_staging_input_queue') )
        bridges.append(_create_queue_bridge('agent_scheduling_queue')    )
        bridges.append(_create_queue_bridge('agent_executing_queue')     )
        bridges.append(_create_queue_bridge('agent_staging_output_queue'))

        # create all notification channels we need (state update notifications,
        # unit unschedule notifications).  Use default addresses, ie. they will
        # bind to to ports 20.000++
        bridges.append(_create_pubsub_bridge('agent_unschedule_pubsub'))
        bridges.append(_create_pubsub_bridge('agent_state_pubsub')     )

        # create all the component types we need.
        # create n instances for each type
        for i in range(2):
            components.append(StagingInput() )
            components.append(Scheduler()    )
            components.append(ExecWorker()   )
            components.append(StagingOutput())
            components.append(Update()       )

        # to watch unit advancement, we also create a state channel subscriber
        # and count unit state changes
        # ----------------------------------------------------------------------
        def count(q, n):

            count = 0
            while True:
                topic, unit = q.get()
                if unit['state'] == 'DONE':
                    count += 1
                    if count >= n:
                        return
        # ----------------------------------------------------------------------
        q = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'agent_state_pubsub', rpu.PUBSUB_SUB)
        q.subscribe('state')
        t = mt.Thread(target=count, args=[q,UNIT_COUNT])
        t.start()

        # FIXME: make sure all communication channels are in place.  This could
        # be replaced with a proper barrier, but not sure if that is worth it...
        time.sleep (1)

        # feed a couple of fresh compute units into the system.  This is what
        # needs to come from the client module / MongoDB.  So, we create a new
        # input to the StagingInput queue, and send units.  The StagingInput
        # components will pull from it and start the pipeline.
        start = time.time()
        intake = rpu.Queue.create(rpu.QUEUE_ZMQ, 'agent_staging_input_queue', rpu.QUEUE_INPUT)
        for i in range(UNIT_COUNT):
            intake.put({'state' : 'STAGING_INPUT', 'id' : i})
        stop = time.time()
        print "intake : %4.2f (%8.2f)" % (stop-start, UNIT_COUNT/(stop-start))

        # wait for the monitoring thread to complete
        t.join()
        stop = time.time()
        print "process: %4.2f (%8.1f)" % (stop-start, UNIT_COUNT/(stop-start))


    except Exception as e:

        print "Exception: %s" % e

    finally:

        # FIXME: let logfiles settle before killing the components
        time.sleep(1)
        os.system('sync')

        # burn the bridges, burn EVERYTHING
        for c in components:
            c.close()

        for b in bridges:
            b.close()


# ==============================================================================
#
agent()
#
# ==============================================================================

