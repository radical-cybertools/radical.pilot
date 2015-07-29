#!/usr/bin/env python 

import os
import zmq
import time
import copy
import Queue
import threading           as mt
import multiprocessing     as mp
import radical.utils       as ru
import radical.pilot.utils as rpu

dh = ru.DebugHelper()

POLL_DELAY = 0.5
FEED_DELAY = 0.1

# ==============================================================================
#
    # we only need one queue manager...
    __metaclass__ = ru.Singleton


    # --------------------------------------------------------------------------
    #
    def __init__(self):
        pass


    # --------------------------------------------------------------------------
    #
    def create_bridge(self, qname):

        return rpu.Queue.create(rpu.QUEUE_ZMQ, qname, rpu.QUEUE_BRIDGE)



# ==============================================================================
#
# globals
#
QM = QueueManager()
N  = 10000


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

        self._name       = type(self).__name__
        self._parent     = os.getpid()
        self._inputs     = list()    # queues to get units from
        self._outputs    = dict()    # queues to send units to
        self._notifiers  = list()    # queues to send unit notifications to
        self._workers    = dict()    # method to send units for work
        self._feed       = mp.Queue()
        self._feed_limit = 2  # max number of units in the feed
        self._terminate  = mt.Event()

        print '%-20s: create' % self._name

        mp.Process.__init__(self)
        self.start()


    # --------------------------------------------------------------------------
    #
    def initialize(self):
        """
        This method must be overloaded by the components.  It is called *once*
        in the context of the main run(), and should be used to set up component
        state before units arrive
        """

        raise NotImplementedError("initialize() is not implemented by %s" % self._name)


    # --------------------------------------------------------------------------
    #
    def stop(self):
        """
        Shut down worker threads and the process itself.
        """
        raise NotImplementedError("stop() is not implemented yet")


    # --------------------------------------------------------------------------
    #
    def feed(self, cus):
        """
        call to let units enter the component proggramatically (opposed to the
        canonical way, via a queue)
        """

        if not isinstance(cus, list):
            cus = [cus]

        for cu in cus:
            self._feed.put(cu)

        for cu in cus:
            # notify arrival (can be out-of-order)
            for notify_output in self._notifiers:
                notify_output.put (cu)


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
                print "%-20s: WARNING: replacing output for %s : %s -> %s" % (self._name, state, self._outputs[state], output)
            else:
                self._outputs[state] = \
                        rpu.Queue.create(rpu.QUEUE_ZMQ, output, rpu.QUEUE_OUTPUT) # FIXME


    # --------------------------------------------------------------------------
    #
    def declare_notifier(self, notifier):

        print "%-20s: declare notifier %s" % (self._name, notifier)
        q = rpu.Queue.create(rpu.QUEUE_ZMQ, notifier, rpu.QUEUE_OUTPUT) # FIXME
        self._notifiers.append(q)


    # --------------------------------------------------------------------------
    #
    def declare_worker(self, states, worker):

        if not isinstance(states, list):
            states = [states]

        for state in states:
            if state in self._workers:
                print "%-20s: WARNING: replace worker for %s (%s)" % (self._name, state, self._workers[state])
            self._workers[state] = worker


    # --------------------------------------------------------------------------
    #
    def _feeder(self):
        """
        cycle over all output queues, and try to gather units.  We only gather
        units until our feed queue has a certain size -- then we let other
        components try to get those CUs.
        """

        while not self._terminate.is_set():

            idle = True

            if self._feed.qsize() < self._feed_limit:
                for [input, states] in self._inputs:
                    cu = input.get_nowait()
                    if cu:
                        idle = False
                        if cu['state'] not in states:
                            print "%-20s: ERROR  : %s: cannot handle unit %s from %s (%s != %s)" %\
                                    (self._name, cu['id'], input.name, cu['state'], states)
                        else:
                            self._feed.put(cu)

                            # notify arrival
                            for notify_output in self._notifiers:
                                notify_output.put (cu)

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
                old_cu    = self._feed.get_nowait()
                old_state = old_cu['state']

                if not old_state in self._workers:
                    print "%-20s: ERROR  : cannot handle state %s: %s" % (self._name, old_state, old_cu)

                new_cu = self._workers[old_state](old_cu)
                if new_cu:
                    self.advance(new_cu)

            except Queue.Empty:
                pass

            if idle:
                time.sleep(POLL_DELAY)


    # --------------------------------------------------------------------------
    #
    def advance(self, cus):
        """
        Units which have been operated upon are pushed down into the queues
        again, only to be picked up by the next component, according to their
        state...
        """

        if not isinstance(cus, list):
            cus = [cus]

        for cu in cus:
            state = cu['state']

            for notify_output in self._notifiers:
                notify_output.put (cu)

            if state not in self._outputs:
                print "%-20s: ERROR  : can't route state %s (%s)" % (self._name, state, self._outputs.keys())
                continue

            # push the unit down the drain
            self._outputs[state].put(cu)


    # --------------------------------------------------------------------------
    #
    def run(self):

        # we are running two threads:
        #   feeder is spinning over all output queues, and if any unit arrives,
        #          it is logged and pushed onto our internal feed queue.
        #   worker is spinning over all units in the feed queue, and works
        #          on them one by one.
        
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

        all_states = ['STAGING_INPUT', 'SCHEDULING', 'EXECUTING', 'STAGING_OUTPUT', 'DONE']

        self.declare_input (all_states, 'agent_update_queue')
        self.declare_worker(all_states, self.work)


    # --------------------------------------------------------------------------
    #
    def work(self, cu):

        print '%s ---> %s' % (cu['id'], cu['state'])
        # do not advance this cu
        return None


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

        self.declare_output('EXECUTING', 'agent_executing_queue')

        self.declare_notifier('agent_update_queue')


    # --------------------------------------------------------------------------
    #
    def work(self, cu):

        cu['state'] = 'EXECUTING'
        return cu



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

        self.declare_input ('SCHEDULING', 'agent_scheduling_queue')
        self.declare_worker('SCHEDULING', self.work_schedule)

        self.declare_input ('STAGING_OUTPUT', 'agent_unscheduling_queue')
        self.declare_worker('STAGING_OUTPUT', self.work_unschedule) # FIXME: don't find route afterwards

        self.declare_output('SCHEDULING', 'agent_executing_queue')

        self.declare_notifier('agent_update_queue')


    # --------------------------------------------------------------------------
    #
    def _alloc(self):

        # find a free core
        if self._cores > 0:
            self._cores -= 1
            print '%-20s: ---> %d' % (self._name, self._cores)
            return True
        return False


    # --------------------------------------------------------------------------
    #
    def _dealloc(self):

        self._cores += 1
        print '%-20s: ===> %d' % (self._name, self._cores)


    # --------------------------------------------------------------------------
    #
    def work_schedule(self, cu):

        wait_pool.append(cu)
        self._reschedule()

        # do not advance this cu
        return None


    # --------------------------------------------------------------------------
    #
    def work_unschedule(self, cu):

        self._dealloc()
        self._reschedule()

        # never advance this cu
        return None

    
    # --------------------------------------------------------------------------
    #
    def _reschedule(self):
        # advance any cu which at this point may fit into the set of cores

        while len(wait_pool):
           if self._alloc():
               cu = wait_pool[0]
               wait_pool.remove(cu)
               cu['state'] = 'SCHEDULING'
               # advance cu
               self.advance(cu)
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
        self.declare_output('STAGING_OUTPUT', 'agent_unscheduling_queue') # FIXME

        self.declare_notifier('agent_update_queue')


    # --------------------------------------------------------------------------
    #
    def work(self, cu):

        # workload
        time.sleep(2)

        cu['state'] = 'STAGING_OUTPUT'
        return cu


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

        self.declare_notifier('agent_update_queue')


    # --------------------------------------------------------------------------
    #
    def work(self, cu):

        cu['state'] = 'DONE'
        return cu



# ==============================================================================
#
def agent():

    bridge_agent_staging_input_bridge  = QM.create_bridge('agent_staging_input_queue')
    bridge_agent_scheduling_bridge     = QM.create_bridge('agent_scheduling_queue')
    bridge_agent_unscheduling_bridge   = QM.create_bridge('agent_unscheduling_queue')
    bridge_agent_executing_bridge      = QM.create_bridge('agent_executing_queue')
    bridge_agent_staging_output_bridge = QM.create_bridge('agent_staging_output_queue')
    bridge_agent_update_bridge         = QM.create_bridge('agent_update_queue')

    staging_input  = StagingInput()
    scheduler      = Scheduler() 
    exec_worker    = ExecWorker()
    staging_output = StagingOutput()
    update         = Update()

    time.sleep(2)

    for i in range(10):
        staging_input.feed({'state' : 'STAGING_INPUT', 'id' : i})
        time.sleep(FEED_DELAY)


agent()

