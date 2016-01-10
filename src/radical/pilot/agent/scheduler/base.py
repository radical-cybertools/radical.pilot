
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import threading

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
# 'enum' for RPs's pilot scheduler types
SCHEDULER_NAME_CONTINUOUS   = "CONTINUOUS"
SCHEDULER_NAME_SCATTERED    = "SCATTERED"
SCHEDULER_NAME_TORUS        = "TORUS"
SCHEDULER_NAME_YARN         = "YARN"


# ==============================================================================
#
class AgentSchedulingComponent(rpu.Component):

    # FIXME: clarify what can be overloaded by Scheduler classes

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        self._slots = None
        self._lrms  = None

        rpu.Component.__init__(self, rpc.AGENT_SCHEDULING_COMPONENT, cfg)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

      # self.declare_input (rps.AGENT_SCHEDULING_PENDING, rpc.AGENT_SCHEDULING_QUEUE)
      # self.declare_worker(rps.AGENT_SCHEDULING_PENDING, self.work)

        self.declare_input (rps.ALLOCATING_PENDING, rpc.AGENT_SCHEDULING_QUEUE)
        self.declare_worker(rps.ALLOCATING_PENDING, self.work)

        self.declare_output(rps.EXECUTING_PENDING,  rpc.AGENT_EXECUTING_QUEUE)

        # we need unschedule updates to learn about units which free their
        # allocated cores.  Those updates need to be issued after execution, ie.
        # by the AgentExecutionComponent.
        self.declare_publisher ('state',      rpc.AGENT_STATE_PUBSUB)
        self.declare_subscriber('unschedule', rpc.AGENT_UNSCHEDULE_PUBSUB, self.unschedule_cb)

        # we create a pubsub pair for reschedule trigger
        self.declare_publisher ('reschedule', rpc.AGENT_RESCHEDULE_PUBSUB)
        self.declare_subscriber('reschedule', rpc.AGENT_RESCHEDULE_PUBSUB, self.reschedule_cb)

        # all components use the command channel for control messages
        self.declare_publisher ('command',    rpc.AGENT_COMMAND_PUBSUB)

        # we declare a clone and a drop callback, so that cores can be assigned
        # to clones, and can also be freed again.
        self.declare_clone_cb(self.clone_cb)
        self.declare_drop_cb (self.drop_cb)

        # when cloning, we fake scheduling via round robin over all cores.
        # These indexes keeps track of the last used core.
        self._clone_slot_idx = 0
        self._clone_core_idx = 0

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

        # communicate successful startup
        self.publish('command', {'cmd' : 'alive',
                                 'arg' : self.cname})


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        # communicate finalization
        self.publish('command', {'cmd' : 'final',
                                 'arg' : self.cname})


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Scheduler.
    #
    @classmethod
    def create(cls, cfg):

        # Make sure that we are the base-class!
        if cls != AgentSchedulingComponent:
            raise TypeError("Scheduler Factory only available to base class!")

        name = cfg['scheduler']

        from .continuous import Continuous
        from .scattered  import Scattered
        from .torus      import Torus
        from .yarn       import Yarn

        try:
            impl = {
                SCHEDULER_NAME_CONTINUOUS : Continuous,
                SCHEDULER_NAME_SCATTERED  : Scattered,
                SCHEDULER_NAME_TORUS      : Torus,
                SCHEDULER_NAME_YARN       : Yarn
            }[name]

            impl = impl(cfg)
            return impl

        except KeyError:
            raise ValueError("Scheduler '%s' unknown or defunct" % name)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        raise NotImplementedError("_configure() not implemented for Scheduler '%s'." % self._cname)


    # --------------------------------------------------------------------------
    #
    def slot_status(self):
        raise NotImplementedError("slot_status() not implemented for Scheduler '%s'." % self._cname)


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, cores_requested):
        raise NotImplementedError("_allocate_slot() not implemented for Scheduler '%s'." % self._cname)


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, opaque_slots):
        raise NotImplementedError("_release_slot() not implemented for Scheduler '%s'." % self._cname)


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

            # schedule this unit, and receive an opaque handle that has meaning to
            # the LRMS, Scheduler and LaunchMethod.
            cu['opaque_slots'] = self._allocate_slot(cu['description']['cores'])

        if not cu['opaque_slots']:
            # signal the CU remains unhandled
            return False

        # got an allocation, go off and launch the process
        self._prof.prof('schedule', msg="allocated", uid=cu['_id'])
        self._log.info("slot status after allocated  : %s" % self.slot_status ())

        return True


    # --------------------------------------------------------------------------
    #
    def reschedule_cb(self, topic, msg):
        # we ignore any passed CU.  In principle the cu info could be used to
        # determine which slots have been freed.  No need for that optimization
        # right now.  This will become interesting once reschedule becomes too
        # expensive.

        cu = msg

        self._prof.prof('reschedule', uid=self._pilot_id)
        self._log.info("slot status before reschedule: %s" % self.slot_status())

        # cycle through wait queue, and see if we get anything running now.  We
        # cycle over a copy of the list, so that we can modify the list on the
        # fly
        for cu in self._wait_pool[:]:

            if self._try_allocation(cu):

                # allocated cu -- advance it
                self.advance(cu, rps.EXECUTING_PENDING, publish=True, push=True)

                # remove it from the wait queue
                with self._wait_lock :
                    self._wait_pool.remove(cu)
                    self._prof.prof('unqueue', msg="re-allocation done", uid=cu['_id'])
            else:
                # Break out of this loop if we didn't manage to schedule a task
                break

        # Note: The extra space below is for visual alignment
        self._log.info("slot status after  reschedule: %s" % self.slot_status ())
        self._prof.prof('reschedule done')


    # --------------------------------------------------------------------------
    #
    def unschedule_cb(self, topic, msg):
        """
        release (for whatever reason) all slots allocated to this CU
        """

        cu = msg
        self._prof.prof('unschedule', uid=cu['_id'])

        if not cu['opaque_slots']:
            # Nothing to do -- how come?
            self._log.warn("cannot unschedule: %s (no slots)" % cu)
            return

        self._log.info("slot status before unschedule: %s" % self.slot_status ())

        # needs to be locked as we try to release slots, but slots are acquired
        # in a different thread....
        with self._slot_lock :
            self._release_slot(cu['opaque_slots'])
            self._prof.prof('unschedule', msg='released', uid=cu['_id'])

        # notify the scheduling thread, ie. trigger a reschedule to utilize
        # the freed slots
        self.publish('reschedule', cu)

        # Note: The extra space below is for visual alignment
        self._log.info("slot status after  unschedule: %s" % self.slot_status ())


    # --------------------------------------------------------------------------
    #
    def clone_cb(self, unit, name=None, mode=None, prof=None, logger=None):

        if mode == 'output':

            # so, this is tricky: we want to clone the unit after scheduling,
            # but at the same time don't want to have all clones end up on the
            # same core -- so the clones should be scheduled to a different (set
            # of) core(s).  But also, we don't really want to schedule, that is
            # why we blow up on output, right?
            #
            # So we fake scheduling.  This assumes the 'self._slots' structure as
            # used by the continuous scheduler, wo will likely only work for
            # this one (FIXME): we walk our own index into the slot structure,
            # and simply assign that core, be it busy or not.
            #
            # FIXME: This method makes no attempt to set 'task_slots', so will
            # not work properly for some launch methods.
            #
            # This is awful.  I mean, really awful.  Like, nothing good can come
            # out of this.  Ticket #902 should be implemented, it will solve
            # this problem much cleaner...

            if prof: prof.prof      ('clone_cb', uid=unit['_id'])
            else   : self._prof.prof('clone_cb', uid=unit['_id'])

            slot = self._slots[self._clone_slot_idx]

            unit['opaque_slots']['task_slots'][0] = '%s:%d' \
                    % (slot['node'], self._clone_core_idx)
          # self._log.debug(' === clone cb out : %s', unit['opaque_slots'])

            if (self._clone_core_idx +  1) < self._lrms_cores_per_node:
                self._clone_core_idx += 1
            else:
                self._clone_core_idx  = 0
                self._clone_slot_idx += 1

                if self._clone_slot_idx >= len(self._slots):
                    self._clone_slot_idx = 0


    # --------------------------------------------------------------------------
    #
    def drop_cb(self, unit, name=None, mode=None, prof=None, logger=None):

        if mode == 'output':
            # we only unscheduler *after* scheduling.  Duh!

            if prof:
                prof.prof('drop_cb', uid=unit['_id'])
            else:
                self._prof.prof('drop_cb', uid=unit['_id'])

            self.unschedule_cb(topic=None, msg=unit)



    # --------------------------------------------------------------------------
    #
    def work(self, cu):

      # self.advance(cu, rps.AGENT_SCHEDULING, publish=True, push=False)
        self.advance(cu, rps.ALLOCATING      , publish=True, push=False)

        # we got a new unit to schedule.  Either we can place it
        # straight away and move it to execution, or we have to
        # put it on the wait queue.
        if self._try_allocation(cu):
            self._prof.prof('schedule', msg="allocation succeeded", uid=cu['_id'])
            self.advance(cu, rps.EXECUTING_PENDING, publish=True, push=True)

        else:
            # No resources available, put in wait queue
            self._prof.prof('schedule', msg="allocation failed", uid=cu['_id'])
            with self._wait_lock :
                self._wait_pool.append(cu)


# ------------------------------------------------------------------------------

