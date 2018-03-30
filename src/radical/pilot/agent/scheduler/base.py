
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import logging
import time
import pprint
import threading

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
# 'enum' for RPs's pilot scheduler types
SCHEDULER_NAME_CONTINUOUS      = "CONTINUOUS"
SCHEDULER_NAME_CONTINUOUS_FIFO = "CONTINUOUS_FIFO"
SCHEDULER_NAME_SCATTERED       = "SCATTERED"
SCHEDULER_NAME_HOMBRE          = "HOMBRE"
SCHEDULER_NAME_TORUS           = "TORUS"
SCHEDULER_NAME_YARN            = "YARN"
SCHEDULER_NAME_SPARK           = "SPARK"


# ==============================================================================
#
class AgentSchedulingComponent(rpu.Component):

    # FIXME: clarify what can be overloaded by Scheduler classes

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.slots = None
        self._lrms = None
        self._uid  = ru.generate_id(cfg['owner'] + '.scheduling.%(counter)s',
                                    ru.ID_CUSTOM)

        self._uniform_waitpool = True   # TODO: move to cfg

        rpu.Component.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self.register_input(rps.AGENT_SCHEDULING_PENDING, 
                            rpc.AGENT_SCHEDULING_QUEUE, self.work)

        self.register_output(rps.AGENT_EXECUTING_PENDING,  
                             rpc.AGENT_EXECUTING_QUEUE)

        # we need unschedule updates to learn about units which free their
        # allocated cores.  Those updates need to be issued after execution, ie.
        # by the AgentExecutionComponent.
        self.register_subscriber(rpc.AGENT_UNSCHEDULE_PUBSUB, self.unschedule_cb)

        # we create a pubsub pair for reschedule trigger
        self.register_publisher (rpc.AGENT_RESCHEDULE_PUBSUB)
        self.register_subscriber(rpc.AGENT_RESCHEDULE_PUBSUB, self.reschedule_cb)

        # The scheduler needs the LRMS information which have been collected
        # during agent startup.  We dig them out of the config at this point.
        self._pilot_id = self._cfg['pilot_id']
        self._lrms_info           = self._cfg['lrms_info']
        self._lrms_lm_info        = self._cfg['lrms_info']['lm_info']
        self._lrms_node_list      = self._cfg['lrms_info']['node_list']
        self._lrms_cores_per_node = self._cfg['lrms_info']['cores_per_node']
        # FIXME: this information is insufficient for the torus scheduler!

        self._wait_pool = list()            # set of units which wait for the resource
        self._wait_lock = threading.RLock() # look on the above set
        self._slot_lock = threading.RLock() # look for slot allocation/deallocation

        # configure the scheduler instance
        self._configure()


    # --------------------------------------------------------------------------
    #
    def _dump_prof(self):
        #  FIXME: this is specific for cprofile use in derived classes, and can
        #         probably be solved cleaner.
        pass

 
    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Scheduler.
    #
    @classmethod
    def create(cls, cfg, session):

        # Make sure that we are the base-class!
        if cls != AgentSchedulingComponent:
            raise TypeError("Scheduler Factory only available to base class!")

        name = cfg['scheduler']

        from .continuous_fifo import ContinuousFifo
        from .continuous      import Continuous
        from .scattered       import Scattered
        from .hombre          import Hombre
        from .torus           import Torus
        from .yarn            import Yarn
        from .spark           import Spark

        try:
            impl = {
                SCHEDULER_NAME_CONTINUOUS_FIFO : ContinuousFifo,
                SCHEDULER_NAME_CONTINUOUS      : Continuous,
                SCHEDULER_NAME_SCATTERED       : Scattered,
                SCHEDULER_NAME_HOMBRE          : Hombre,
                SCHEDULER_NAME_TORUS           : Torus,
                SCHEDULER_NAME_YARN            : Yarn,
                SCHEDULER_NAME_SPARK           : Spark
            }[name]

            impl = impl(cfg, session)
            return impl

        except KeyError:
            raise ValueError("Scheduler '%s' unknown or defunct" % name)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        raise NotImplementedError("_configure() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def slot_status(self):
        raise NotImplementedError("slot_status() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, cores_requested):
        raise NotImplementedError("_allocate_slot() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, opaque_slots):
        raise NotImplementedError("_release_slot() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, cu):
        """
        Attempt to allocate cores for a specific CU.  If it succeeds, send the
        CU off to the ExecutionWorker.
        """

        # needs to be locked as we try to acquire slots, but slots are freed
        # in a different thread.  But we keep the lock duration short...
        with self._slot_lock :

            self._prof.prof('schedule_try', uid=cu['uid'])
            # schedule this unit, and receive an opaque handle that has meaning to
            # the LRMS, Scheduler and LaunchMethod.
            cu['opaque_slots'] = self._allocate_slot(cu['description']['cores'])

        if not cu['opaque_slots']:
            # signal the CU remains unhandled
            self._prof.prof('schedule_fail', uid=cu['uid'])
            return False

        # got an allocation, go off and launch the process
        self._prof.prof('schedule_ok', uid=cu['uid'])

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("after  allocate   %s: %s", cu['uid'], 
                            self.slot_status())

            self._log.debug("%s [%s] : %s", cu['uid'],
                            cu['description']['cores'],
                            pprint.pformat(cu['opaque_slots']))

        return True


    # --------------------------------------------------------------------------
    #
    def reschedule_cb(self, topic, msg):

        # we ignore any passed CU.  In principle the cu info could be used to
        # determine which slots have been freed.  No need for that optimization
        # right now.  This will become interesting once reschedule becomes too
        # expensive.
        # FIXME: optimization

        cu = msg

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("before reschedule %s: %s", cu['uid'], 
                            self.slot_status())

        # cycle through wait queue, and see if we get anything running now.  We
        # cycle over a copy of the list, so that we can modify the list on the
        # fly
        for cu in self._wait_pool[:]:

            if self._try_allocation(cu):

                # allocated cu -- advance it
                self.advance(cu, rps.AGENT_EXECUTING_PENDING, publish=True, push=True)

                # remove it from the wait queue
                with self._wait_lock :
                    self._wait_pool.remove(cu)

            else:
                # Break out of this loop if we didn't manage to schedule a task
                # FIXME: this assumes that no smaller or otherwise more suitable
                #        CUs come after this one - which is naive, ie. wrong.
                # NOTE:  This assumption does indeed break for the fifo
                #        scheduler, so we disable this now.  But:
                if self._uniform_waitpool:
                    break
                else:
                    pass

        # Note: The extra space below is for visual alignment
        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("after  reschedule %s: %s", cu['uid'], self.slot_status())

        return True


    # --------------------------------------------------------------------------
    #
    def unschedule_cb(self, topic, msg):
        """
        release (for whatever reason) all slots allocated to this CU
        """

        cu = msg
        self._prof.prof('unschedule_start', uid=cu['uid'])

        if not cu['opaque_slots']:
            # Nothing to do -- how come?
            self._log.error("cannot unschedule: %s (no slots)" % cu)
            return True

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("before unschedule %s: %s", cu['uid'], self.slot_status())

        # needs to be locked as we try to release slots, but slots are acquired
        # in a different thread....
        with self._slot_lock :
            self._release_slot(cu['opaque_slots'])
            self._prof.prof('unschedule_stop', uid=cu['uid'])

        # notify the scheduling thread, ie. trigger a reschedule to utilize
        # the freed slots
        self.publish(rpc.AGENT_RESCHEDULE_PUBSUB, cu)

        # Note: The extra space below is for visual alignment
        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("after  unschedule %s: %s", cu['uid'], self.slot_status())

        return True


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_SCHEDULING, publish=True, push=False)

        for unit in units:

            self._handle_unit(unit)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, cu):

        # we got a new unit to schedule.  Either we can place it
        # straight away and move it to execution, or we have to
        # put it on the wait queue.
        if self._try_allocation(cu):
            self.advance(cu, rps.AGENT_EXECUTING_PENDING, publish=True, push=True)

        else:
            # No resources available, put in wait queue
            with self._wait_lock :
                self._wait_pool.append(cu)


# ------------------------------------------------------------------------------

