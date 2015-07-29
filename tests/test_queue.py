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


# ==============================================================================
#
class ComponentBase(mp.Process):
    """
    This class provides the basic structure for any RP component which operates
    on (compute) units.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        self._name        = type(self).__name__
        self._parent      = os.getpid()
        self._inputs      = list()    # queues to get units from
        self._outputs     = dict()    # queues to send units to
        self._publishers  = dict()    # channels to send notifications to
        self._subscribers = dict()    # callbacks for notifications
        self._workers     = dict()    # method to send units for work
        self._feed        = mp.Queue()
        self._feed_limit  = 2  # max number of units in the feed
        self._terminate   = mt.Event()
        self._debug       = False

        if 'RADICAL_DEBUG' in os.environ:
            self._debug = True

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
        pass

    # --------------------------------------------------------------------------
    #
    def close(self):
        """
        Shut down worker threads and the process itself.
        """
        try:
            self.terminate()
        except Exception as e:
            print "kill error: %s" % e


    # --------------------------------------------------------------------------
    #
    def _log(self, msg):

        if self._debug:
            tid = mt.current_thread().name
            with open("component.%s.%d.log" % (self._name, os.getpid()), 'a') as f:
                f.write("%15.5f: %s\n" % (time.time(), msg))
                f.flush()
          # print("%10.2f: %s\n" % (time.time(), msg))


    # --------------------------------------------------------------------------
    #
    def feed(self, units):
        """
        call to let units enter the component proggramatically (opposed to the
        canonical way, via a queue)
        """

        if not isinstance(units, list):
            units = [units]

        for unit in units:
            self._feed.put(unit)


    # --------------------------------------------------------------------------
    #
    def declare_input(self, states, input):

        if not isinstance(states, list):
            states = [states]

        for state in states:
            print "%-20s: declare input %s for %s" % (self._name, input, state)

        q = rpu.Queue.create(rpu.QUEUE_ZMQ, input, rpu.QUEUE_OUTPUT)
        self._inputs.append([q, states])


    # --------------------------------------------------------------------------
    #
    def declare_output(self, states, output):

        if not isinstance(states, list):
            states = [states]

        for state in states:
            
            print "%-20s: declare output %s for %s" % (self._name, output, state)
            if state in self._outputs:
                print "WARNING: %s replaces output for %s : %s -> %s" % (self._name, state, self._outputs[state], output)
            else:
                if not output:
                    # this indicates a final state
                    self._outputs[state] = None
                else:
                    # non-final state, ie. we have a queue to push to
                    self._outputs[state] = \
                            rpu.Queue.create(rpu.QUEUE_ZMQ, output, rpu.QUEUE_INPUT)


    # --------------------------------------------------------------------------
    #
    def declare_publisher(self, topic, channel):

        q = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, channel, rpu.PUBSUB_PUB)
        
        if topic not in self._publishers:
            self._publishers[topic] = list()
        
        self._publishers[topic].append(q)

        print "%-20s: declare publisher %s: %s" % (self._name, topic, channel)


    # --------------------------------------------------------------------------
    #
    def declare_subscriber(self, topic, channel, cb):

        # ----------------------------------------------------------------------
        def _subscriber(q, callback):

            while True:
                topic, msg = q.get()
                if topic and msg:
                    callback (topic, msg)
        # ----------------------------------------------------------------------

        q = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, channel, rpu.PUBSUB_SUB)
        q.subscribe(topic)

        t = mt.Thread(target=_subscriber, args=[q,cb])
        t.start()
        # FIXME: shutdown

        print "%-20s: declare subscriber %s: %s" % (self._name, topic, channel)


    # --------------------------------------------------------------------------
    #
    def declare_worker(self, states, worker):

        if not isinstance(states, list):
            states = [states]

        for state in states:
            if state in self._workers:
                print "WARNING: %s replaces worker for %s (%s)" % (self._name, state, self._workers[state])
            self._workers[state] = worker


    # --------------------------------------------------------------------------
    #
    def _feeder(self):
        """
        cycle over all output queues, and try to gather units.  We only gather
        units until our feed queue has a certain size -- then we let other
        components try to get those units.
        """

        while not self._terminate.is_set():

            idle = True

            if self._feed.qsize() < self._feed_limit:
                for [input, states] in self._inputs:
                    unit = input.get_nowait()
                    if unit:
                        idle = False
                        if unit['state'] not in states:
                            print "ERROR  : %s: cannot handle unit %s from %s (%s != %s)" %\
                                    (self._name, unit['id'], input.name, unit['state'], states)
                        else:
                            self._feed.put(unit)

                            # notify arrival
                            self.publish('state', unit)

            if idle:
                # avoid busy wait
                time.sleep(POLL_DELAY)


    # --------------------------------------------------------------------------
    #
    def _worker(self):
        """
        cycle again and again over all units in the self._feeder queue, and for
        all of them, call the work method which is registered for the respective
        state.
        """

        while not self._terminate.is_set():

            idle = True

            try:
                unit    = self._feed.get_nowait()
                state = unit['state']

                if not state in self._workers:
                    print "ERROR  : %s cannot handle state %s: %s" % (self._name, state, unit)

                self._log('working on %s [%s]' % (unit['id'], unit['state']))
                self._workers[state](unit)

            except Queue.Empty:
                pass

            if idle:
                time.sleep(POLL_DELAY)


    # --------------------------------------------------------------------------
    #
    def advance(self, units):
        """
        Units which have been operated upon are pushed down into the queues
        again, only to be picked up by the next component, according to their
        state...
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
              # print '%s %s ===| %s' % ('state', unit['id'], unit['state'])
                continue

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

        for unit in units:

            # send notifications
            if topic in self._publishers:
                for p in self._publishers[topic]:
                    p.put (topic, unit)


    # --------------------------------------------------------------------------
    #
    def run(self):

        # we are running two threads:
        #   feeder is spinning over all output queues, and if any unit arrives,
        #          it is logged and pushed onto our internal feed queue.
        #   worker is spinning over all units in the feed queue, and works
        #          on them one by one.
        
        self._log('initialize')

        self.initialize()

        # start feeder and worker threads
        self._feeder_thread = mt.Thread (target=self._feeder)
        self._worker_thread = mt.Thread (target=self._worker)

        print "%-20s: run" % self._name

        self._feeder_thread.start()
        self._worker_thread.start()

        print "%-20s: running" % self._name

        self._feeder_thread.join()
        self._worker_thread.join()

        self.finalize()


# ==============================================================================
#
class Update(ComponentBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        ComponentBase.__init__(self)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._stats = dict()
        self._stats['all'] = 0
        self.declare_subscriber('state', 'agent_state_pubsub', self.state_cb)


    # --------------------------------------------------------------------------
    #
    def state_cb(self, topic, unit):

        state = unit['state']
        if state not in self._stats:
            self._stats[state] = 0
        self._stats[state] += 1
        self._stats['all'] += 1

        if self._stats['all'] > 600:
            self._log(pprint.pformat(self._stats))
      # print '%s %s ---> %s [%s]' % (topic, unit['id'], unit['state'], self._stats['all'])


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        self._log('finalize')
        self._log(pprint.pformat(self._stats))


# ==============================================================================
#
class StagingInput(ComponentBase):

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

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        ComponentBase.__init__(self)


    # --------------------------------------------------------------------------
    #
    def initialize(self):
        self._cores     = 5
        self._wait_pool = list()
        self._wait_lock = mt.RLock()

        self.declare_input ('SCHEDULING', 'agent_scheduling_queue')
        self.declare_worker('SCHEDULING', self.work_schedule)

        self.declare_output('EXECUTING',  'agent_executing_queue')

        self.declare_publisher ('state',      'agent_state_pubsub')
        self.declare_subscriber('unschedule', 'agent_unschedule_pubsub', self.unschedule_cb)


    # --------------------------------------------------------------------------
    #
    def _alloc(self):

        # find a free core
        if self._cores > 0:
            self._cores -= 1
            self._log('%-20s: ---> %d' % (self._name, self._cores))
            return True
        return False


    # --------------------------------------------------------------------------
    #
    def _dealloc(self):

        self._cores += 1
        self._log('%-20s: ===> %d' % (self._name, self._cores))


    # --------------------------------------------------------------------------
    #
    def work_schedule(self, unit):

        with self._wait_lock:
            self._wait_pool.append(unit)
        self._reschedule()


    # --------------------------------------------------------------------------
    #
    def unschedule_cb(self, topic, unit):

        if unit['state'] in ['STAGING_OUTPUT', 'DONE', 'FAILED', 'CANCELED']:
            self._dealloc()
            self._reschedule()

    
    # --------------------------------------------------------------------------
    #
    def _reschedule(self):
        # advance any unit which at this point may fit into the set of cores

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

        # workload
      # time.sleep(1)

        unit['state'] = 'STAGING_OUTPUT'
        self.publish('unschedule', unit)
        self.advance(unit)


# ==============================================================================
#
class StagingOutput(ComponentBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        ComponentBase.__init__(self)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self.declare_input ('STAGING_OUTPUT', 'agent_staging_output_queue')
        self.declare_worker('STAGING_OUTPUT', self.work)

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

    try:

        # create all communication bridges we need.  Use the default addresses,
        # ie. they will bind to localhost to ports 10.000++
        def _create_queue_bridge(qname):
            return rpu.Queue.create(rpu.QUEUE_ZMQ, qname, rpu.QUEUE_BRIDGE)

        staging_input_queue  = _create_queue_bridge('agent_staging_input_queue')
        scheduling_queue     = _create_queue_bridge('agent_scheduling_queue')
        executing_queue      = _create_queue_bridge('agent_executing_queue')
        staging_output_queue = _create_queue_bridge('agent_staging_output_queue')

        # create all notification channels we need (state update notifications,
        # unit unschedule notifications).  Use default addresses, ie. they will
        # bind to to ports 20.000++
        def _create_pubsub_bridge(channel):
            return rpu.Pubsub.create(rpu.PUBSUB_ZMQ, channel, rpu.PUBSUB_BRIDGE)

        unschedule_pubsub = _create_pubsub_bridge('agent_unschedule_pubsub')
        state_pubsub      = _create_pubsub_bridge('agent_state_pubsub')

        # create all the components we need.  Those run in separate processes.
        i1_staging_input  = StagingInput()
        i1_scheduler      = Scheduler() 
        i1_exec_worker    = ExecWorker()
        i1_staging_output = StagingOutput()
        i1_update         = Update()

      # i2_staging_input  = StagingInput()
      # i2_scheduler      = Scheduler() 
      # i2_exec_worker    = ExecWorker()
      # i2_staging_output = StagingOutput()
      # i2_update         = Update()

        # to watch unit advancement, we also create a state channel subscriber
        # and count unit state changes
        # ----------------------------------------------------------------------
        def count(q, n):

            count = 0

            while True:
                  # print '?'
                    topic, unit = q.get()
                  # print count, n, unit
                    if unit['state'] == 'DONE':
                        count += 1
                        print ' %d out of %d units are DONE (%s)' % (count, n, unit['id'])
                        if count > n:
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
        intake = rpu.Queue.create(rpu.QUEUE_ZMQ, 'agent_staging_input_queue', rpu.QUEUE_INPUT)

        start = time.time()
        for i in range(UNIT_COUNT):
            print 'intake'
            intake.put({'state' : 'STAGING_INPUT', 'id' : i})

        t.join()
        stop = time.time()
        print "%.2f (%.1f)" % (stop-start, UNIT_COUNT/(stop-start))



    except Exception as e:

        print "Exception: %s" % e

    finally:

        # FIXME: let logfiles settle before killing the components
        time.sleep(1)
        os.system('sync')

        # burn the bridges, burn EVERYTHING
        i1_staging_input     .close()
        i1_scheduler         .close()
        i1_exec_worker       .close()
        i1_staging_output    .close()
        i1_update            .close()

      # i2_staging_input     .close()
      # i2_scheduler         .close()
      # i2_exec_worker       .close()
      # i2_staging_output    .close()
      # i2_update            .close()

        staging_input_queue  .close()
        scheduling_queue     .close()
        executing_queue      .close()
        staging_output_queue .close()

        unschedule_pubsub    .close()
        state_pubsub         .close()


# ==============================================================================
#
agent()
# 
# ==============================================================================

