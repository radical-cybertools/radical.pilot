
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
SCHEDULER_NAME_CONTINUOUS   = "CONTINUOUS"
SCHEDULER_NAME_SCATTERED    = "SCATTERED"
SCHEDULER_NAME_TORUS        = "TORUS"
SCHEDULER_NAME_YARN         = "YARN"
SCHEDULER_NAME_SPARK        = "SPARK"

# ------------------------------------------------------------------------------
#
# An RP agent scheduler will place incoming units onto a set of cores and gpus.
# To do so, the scheduler needs three pieces of information:
#
#   - the layout of the resource (nodes, cores, gpus)
#   - the current state of those (what cores/gpus are used by other units)
#   - the requirements of the unit (single/multi node, cores, gpus)
#
# The first part (layout) is provided by the LRMS, in the form of a nodelist:
#
#    nodelist = [{name : 'node_1', cores: 16, gpus : 2},
#                {name : 'node_2', cores: 16, gpus : 2},
#                ...
#               ]
#
# That is then mapped into an internal representation, which is really the same
# but allows to keep track of resource usage, by setting the fields to
# `rpc.FREE == '-'` or `rpc.BUSY == '#'`:
#
#    nodelist = [{name : 'node_1', cores: [----------------], gpus : [--]},
#                {name : 'node_2', cores: {----------------], gpus : [--]},
#                ...
#               ]
#
# When allocating a set of resource for a unit (2 cores, 1 gpu), we can now
# record those as used:
#
#    nodelist = [{name : 'node_1', cores: [##--------------], gpus : [#-]},
#                {name : 'node_2', cores: {----------------], gpus : [--]},
#                ...
#               ]
#
# This solves the second part from our list above.  The third part, unit
# requirements, are obtained from the unit dict passed for scheduling: the unit
# description contains requests for `cores` and `gpus`, and also flags the use
# of `mpi`.
#
# Note that the unit dict will also contain `threads_per_proc`: the scheduler
# will have to make sure that for each process to be placed, the  given number
# of additional cores are available * and reserved* to create threads on.  The
# threads are created by the application.  Note though that this implies that
# the launcher must avoid core pinnin, as otherwise the OS could not be place
# the threads on the respective cores.
#
# The scheduler algorithm will then attempt to find a suitable set of cores and
# gpus in the nodelist into which the CU can be placed.  It will mark those as
# `rpc.BUSY`, and attach the set of cores/gpus to the CU dictionary, as (here
# for system with 8 cores & 1 gpu per node):
#
#     cu = { ...
#       'cpu_processes'    : 4,
#       'cpu_threads'      : 2,
#       'gpu_processes     : 2,
#       'slots' :
#       {                 # [[nodename, [node_uid], [core indexes],   [gpu idx]]]
#         'nodes'         : [[node_1,   node_uid_1, [[0, 2], [4, 6]], [[0]    ]],
#                            [node_2,   node_uid_2, [[1, 3], [5, 7]], [[0]    ]]],
#         'cores_per_node': 8,
#         'gpus_per_node' : 1,
#         'lm_info'       : { ... }
#       }
#     }
#
# The repsective launch method is expected to create processes on the respective
# given set of cores (nodename_1, cores 0 and 4; nodename_2, cores 1 and 5), and
# on the respective GPUs.  The other reserved cores are for the application to
# spawn threads on (`cpu_threads=2`).
#
# A scheduler MAY attach other information to the `slots` structure, with the
# intent to support the launch methods to enact the placement decition made by
# the scheduler.  In fact, a scheduler may use a completely different slot
# structure than above - but then is likely bound to a specific launch method
# which can interpret that structure.  A notable example is the BG/Q torus
# scheduler which will only work in combination with the dplace launch methods.
# `lm_info` is an opaque field which allows to communicate specific settings
# from the lrms to the launch method
#
# FIXME: `lm_info` should be communicated to the LM instances in creation, not
#        as part of the slots.  Its constant anyway, as lm_info is set only
#        once during lrms startup.
#
# NOTE:  While the nodelist resources are listed as strings above, we in fact
#        use a list of integers, to simplify some operations, and to
#        specifically avoid string   copies on manipulations.  We only convert
#        to a stringlist for visual representation (`self._slot_status()`).
#
# NOTE:  the scheduler will allocate one core per node and GPU, as some startup
#        methods only allow process placements to *cores*, even if GPUs are
#        present and requested (hi aprun).  We should make this decision
#        dependent on the chosen executor - but at this point we don't have this
#        information (at least not readily available).
#
# FIXME: clarify what can be overloaded by Scheduler classes
#
# TODO:  use named tuples for the slot structure to make the code more readable,
#        specifically for the LMs.
#

# ==============================================================================
#
class AgentSchedulingComponent(rpu.Component):

    # FIXME: clarify what can be overloaded by Scheduler classes

    # --------------------------------------------------------------------------
    #
    # the deriving schedulers should in general have the following structure in
    # self.nodes:
    #
    #   self.nodes = [
    #     { 'name'  : 'name-of-node',
    #       'cores' : '###---##-##-----',  # 16 cores, free/busy markers
    #       'gpus'  : '--',                #  2 GPUs,  free/busy markers
    #     }, ...
    #   ]
    #
    # The free/busy markers are defined in rp.constants.py, and are `-` and `#`,
    # respectively.  Some schedulers may need a more elaborate structures - but
    # where the above is suitable, it should be used for code consistency.
    #
    #
    def __init__(self, cfg, session):

        self.nodes = None
        self._lrms = None

        self._uid = ru.generate_id('agent.scheduling.%(counter)s', ru.ID_CUSTOM)

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
        self._lrms_gpus_per_node  = self._cfg['lrms_info']['gpus_per_node']
        # FIXME: this information is insufficient for the torus scheduler!
        # FIXME: GPU

        self._wait_pool = list()            # set of units which wait for the resource
        self._wait_lock = threading.RLock() # look on the above set
        self._slot_lock = threading.RLock() # look for slot allocation/deallocation

        # configure the scheduler instance
        self._configure()
        self._log.debug("slot status after  initialization        : %s", self.slot_status())


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

        from .continuous import Continuous
        from .scattered  import Scattered
        from .torus      import Torus
        from .yarn       import Yarn
        from .spark      import Spark

        try:
            impl = {
                SCHEDULER_NAME_CONTINUOUS : Continuous,
                SCHEDULER_NAME_SCATTERED  : Scattered,
                SCHEDULER_NAME_TORUS      : Torus,
                SCHEDULER_NAME_YARN       : Yarn,
                SCHEDULER_NAME_SPARK      : Spark
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
    def _allocate_slot(self, cud):
        raise NotImplementedError("_allocate_slot() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, slots):
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
            cu['slots'] = self._allocate_slot(cu['description'])

        if not cu['slots']:
            # signal the CU remains unhandled
            self._prof.prof('schedule_fail', uid=cu['uid'])
            return False

        # got an allocation, go off and launch the process
        self._prof.prof('schedule_ok', uid=cu['uid'])

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("after  allocate   %s: %s", cu['uid'],
                            self.slot_status())

        self._log.debug("%s [%s/%s] : %s [%s]", cu['uid'],
                        cu['description']['cpu_processes'],
                        cu['description']['gpu_processes'],
                        pprint.pformat(cu['slots']))
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
                break

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

        if not cu['slots']:
            # Nothing to do -- how come?
            self._log.error("cannot unschedule: %s (no slots)" % cu)
            return True

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("before unschedule %s: %s", cu['uid'], self.slot_status())

        # needs to be locked as we try to release slots, but slots are acquired
        # in a different thread....
        with self._slot_lock :
            self._release_slot(cu['slots'])
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

