
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
#
# 'enum' for RPs's pilot scheduler types
#
SCHEDULER_NAME_CONTINUOUS   = "CONTINUOUS"
SCHEDULER_NAME_SCATTERED    = "SCATTERED"
SCHEDULER_NAME_TORUS        = "TORUS"
SCHEDULER_NAME_YARN         = "YARN"
SCHEDULER_NAME_SPARK        = "SPARK"

# ------------------------------------------------------------------------------
#
# An RP agent scheduler will place incoming units onto a set of cores and gpus.
#
# This is the agent scheduler base class.  It provides the framework for
# implementing diverse scheduling algorithms and mechanisms, tailored toward
# specific workload types, resource configurations, batch systems etc.
#
# The base class provides the following functionality to the implementations:
#
#   - obtain configuration settings from config files and environments
#   - create aself._nodes list to represent available resources; 
#   - general control and data flow:
#
#       # main loop
#       self._handle_unit(unit):  # unit arrives
#         try_allocation(unit)   # placement is attempted
#         if success:
#            advance(unit)       # pass unit to executor
#         else:
#            wait.append(unit)   # place unit in a wait list
#
#   - event management:
#       - unit finished execution, cores/gpus can be reused
#         This triggers an 'unschedule' (free resources) and reschedule (check
#         waitlist if any waiting unit can be placed now).
#
# A scheduler implementation will derive from this base class, and overload the
# following three methods:
#
#
#   _configure():
#     - make sure that the base class configuration is usable
#     - do any additional configuration
#
#   _allocate_slot(cud):
#     - given a unit description, find and return a suitable allocation
#
#   _release_slot(slots):
#     - release the given allocation
#
#
# The scheduler needs (in the general case) three pieces of information:
#
#   - the layout of the resource (nodes, cores, gpus);
#   - the current state of those (what cores/gpus are used by other units)
#   - the requirements of the unit (single/multi node, cores, gpus)
#
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
# description contains requests for `cores` and `gpus`.
#
# Note that the unit description will also list the number of processes and
# threads for cores and gpus, and also flags the use of `mpi`, 'openmp', etc.
# The scheduler will have to make sure that for each process to be placed, the
# given number of additional cores are available *and reserved* to create
# threads on.  The threads are created by the application.  Note though that
# this implies that the launcher must pin cores very carefully, to not constrain
# the thread creation for the application.
#
# The scheduler algorithm will then attempt to find a suitable set of cores and
# gpus in the nodelist into which the CU can be placed.  It will mark those as
# `rpc.BUSY`, and attach the set of cores/gpus to the CU dictionary, as (here
# for system with 8 cores & 1 gpu per node):
#
#     cu = { ...
#       'cpu_processes'    : 4,
#       'cpu_process_type' : 'mpi',
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
# from the lrms to the launch method.
#
# FIXME: `lm_info` should be communicated to the LM instances in creation, not
#        as part of the slots.  Its constant anyway, as lm_info is set only
#        once during lrms startup.
#
# NOTE:  While the nodelist resources are listed as strings above, we in fact
#        use a list of integers, to simplify some operations, and to
#        specifically avoid string copies on manipulations.  We only convert
#        to a stringlist for visual representation (`self.slot_status()`).
#
# NOTE:  The scheduler will allocate one core per node and GPU, as some startup
#        methods only allow process placements to *cores*, even if GPUs are
#        present and requested (hi aprun).  We should make this decision
#        dependent on the `lm_info` field - but at this point we don't have this
#        information (at least not readily available).
#
# TODO:  use named tuples for the slot structure to make the code more readable,
#        specifically for the LMs.
#

# ==============================================================================
#
class AgentSchedulingComponent(rpu.Component):

    # --------------------------------------------------------------------------
    #
    # the deriving schedulers should in general have the following structure in
    # self.odes:
    #
    #   self.nodes = [
    #     { 'name'  : 'name-of-node',
    #       'uid'   : 'uid-of-node',
    #       'cores' : '###---##-##-----',  # 16 cores, free/busy markers
    #       'gpus'  : '--',                #  2 GPUs,  free/busy markers
    #     }, ...
    #   ]
    #
    # The free/busy markers are defined in rp.constants.py, and are `-` and `#`,
    # respectively.  Some schedulers may need a more elaborate structures - but
    # where the above is suitable, it should be used for code consistency.
    #
    def __init__(self, cfg, session):

        self.nodes = None
        self._lrms = None

        self._uid = ru.generate_id('agent.scheduling.%(counter)s', ru.ID_CUSTOM)

        rpu.Component.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    # Once the component process is spawned, `initialize_child()` will be called
    # before control is given to the component's main loop.
    #
    def initialize_child(self):

        # register unit input channels
        self.register_input(rps.AGENT_SCHEDULING_PENDING,
                            rpc.AGENT_SCHEDULING_QUEUE, self._schedule_units)

        # register unit output channels
        self.register_output(rps.AGENT_EXECUTING_PENDING,
                             rpc.AGENT_EXECUTING_QUEUE)

        # we need unschedule updates to learn about units for which to free the
        # allocated cores.  Those updates MUST be issued after execution, ie.
        # by the AgentExecutionComponent.
        self.register_subscriber(rpc.AGENT_UNSCHEDULE_PUBSUB, self.unschedule_cb)

        # we don't want the unschedule above to compete with actual
        # scheduling attempts, so we move the re-scheduling of units from the
        # wait pool into a separate thread (ie. register a separate callback).
        # This is triggered by the unscheduled_cb.
        #
        # NOTE: we could use a local queue here.  Using a zmq bridge goes toward
        #       an distributed scheduler, and is also easier to implement right
        #       now, since `Component` provides the right mechanisms...
        self.register_publisher (rpc.AGENT_RESCHEDULE_PUBSUB)
        self.register_subscriber(rpc.AGENT_RESCHEDULE_PUBSUB, self.reschedule_cb)

        # The scheduler needs the LRMS information which have been collected
        # during agent startup.  We dig them out of the config at this point.
        #
        # NOTE: this information is insufficient for the torus scheduler!
        self._pilot_id = self._cfg['pilot_id']
        self._lrms_info           = self._cfg['lrms_info']
        self._lrms_lm_info        = self._cfg['lrms_info']['lm_info']
        self._lrms_node_list      = self._cfg['lrms_info']['node_list']
        self._lrms_cores_per_node = self._cfg['lrms_info']['cores_per_node']
        self._lrms_gpus_per_node  = self._cfg['lrms_info']['gpus_per_node']

        # create and initialize the wait pool
        self._wait_pool = list()             # pool of waiting units
        self._wait_lock = threading.RLock()  # look on the above pool
        self._slot_lock = threading.RLock()  # lock slot allocation/deallocation

        # initialize the node list to be used by the scheduler.  A scheduler
        # instance may decide to overwrite or extend this structure.
        self.nodes = []
        for node, node_uid in self._lrms_node_list:
            self.nodes.append({
                'name' : node,
                'uid'  : node_uid,
                'cores': [rpc.FREE] * self._lrms_cores_per_node,
                'gpus' : [rpc.FREE] * self._lrms_gpus_per_node
            })

        # configure the scheduler instance
        self._configure()
        self._log.debug("slot status after  init      : %s", self.slot_status())


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate instance for the scheduler.
    #
    @classmethod
    def create(cls, cfg, session):

        # make sure that we are the base-class!
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
    # Change the reserved state of slots (rpc.FREE or rpc.BUSY)
    #
    # NOTE: any scheduler implementation which uses a different nodelist
    #       structure MUST overload this method.
    def _change_slot_states(self, slots, new_state):

        # FIXME: we don't know if we should change state for cores or gpus...
      # self._log.debug('slots: %s', pprint.pformat(slots))

        for node_name, node_uid, cores, gpus in slots['nodes']:

            # Find the entry in the the slots list
            node = (n for n in self.nodes if n['uid'] == node_uid).next()
            assert(node)

            for cslot in cores:
                for core in cslot:
                    node['cores'][core] = new_state

            for gslot in gpus:
                for gpu in gslot:
                    node['gpus'][gpu] = new_state


    # --------------------------------------------------------------------------
    #
    # NOTE: any scheduler implementation which uses a different nodelist
    #       structure MUST overload this method.
    def slot_status(self):
        '''
        Returns a multi-line string corresponding to the status of the node list
        '''

        ret = "|"
        for node in self.nodes:
            for core in node['cores']:
                if core == rpc.FREE  : ret += '-'
                else                 : ret += '#'
            ret += ':'
            for gpu in node['gpus']  :
                if gpu == rpc.FREE   : ret += '-'
                else                 : ret += '#'
            ret += '|'

        return ret


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        raise NotImplementedError("_configure() missing for '%s'" % self.uid)


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
    def _schedule_units(self, units):
        '''
        This is the main callback of the component, which is calledfor any
        incoming (set of) unit(s).  Units arriving here must always be in
        `AGENT_SCHEDULING_PENDING` state, and must always leave in either
        `AGENT_EXECUTING_PENDING` or in a FINAL state (`FAILED` or `CANCELED`).
        While handled by this method, the units will be in `AGENT_SCHEDULING`
        state.
        '''

        # unify handling of bulks / non-bulks
        if not isinstance(units, list):
            units = [units]

        # advance state, publish state change, do not push unit out.
        self.advance(units, rps.AGENT_SCHEDULING, publish=True, push=False)

        for unit in units:

            # we got a new unit to schedule.  Either we can place it
            # straight away and move it to execution, or we have to
            # put it in the wait pool.
            if self._try_allocation(cu):
                # we could schedule the unit - advance its state, notify worls
                # about the state change, and push the unit out toward the next
                # component.
                self.advance(cu, rps.AGENT_EXECUTING_PENDING, 
                             publish=True, push=True)
            else:
                # no resources available, put in wait queue
                with self._wait_lock :
                    self._wait_pool.append(cu)


    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, cu):
        """
        attempt to allocate cores/gpus for a specific CU.
        """

        # needs to be locked as we try to acquire slots here, but slots are
        # freed in a different thread.  But we keep the lock duration short...
        with self._slot_lock :

            self._prof.prof('schedule_try', uid=cu['uid'])
            cu['slots'] = self._allocate_slot(cu['description'])


        # the lock is freed here
        if not cu['slots']:

            # signal the CU remains unhandled (Fales signals that failure)
            self._prof.prof('schedule_fail', uid=cu['uid'])
            return False


        # got an allocation, we can go off and launch the process
        self._prof.prof('schedule_ok', uid=cu['uid'])

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("after  allocate   %s: %s", cu['uid'],
                            self.slot_status())

        self._log.debug("%s [%s/%s] : %s [%s]", cu['uid'],
                        cu['description']['cpu_processes'],
                        cu['description']['gpu_processes'],
                        pprint.pformat(cu['slots']))
        # True signals success
        return True


    # --------------------------------------------------------------------------
    #
    def unschedule_cb(self, topic, msg):
        """
        release (for whatever reason) all slots allocated to this CU
        """

        cu = msg

        if not cu['slots']:
            # Nothing to do -- how come?
            self._log.error("cannot unschedule: %s (no slots)" % cu)
            return True

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("before unschedule %s: %s", cu['uid'], self.slot_status())

        # needs to be locked as we try to release slots, but slots are acquired
        # in a different thread....
        with self._slot_lock :
            self._prof.prof('unschedule_start', uid=cu['uid'])
            self._release_slot(cu['slots'])
            self._prof.prof('unschedule_stop',  uid=cu['uid'])

        # notify the rescheduling thread, ie. trigger a reschedule to utilize
        # the freed slots for units waiting in the wait pool.
        self.publish(rpc.AGENT_RESCHEDULE_PUBSUB, cu)

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("after  unschedule %s: %s", cu['uid'], self.slot_status())

        # return True to keep the cb registered
        return True


    # --------------------------------------------------------------------------
    #
    def reschedule_cb(self, topic, msg):
        '''
        This cb is triggered after a unit's resources became available again, so
        we can attempt to schedule units from the wait pool.
        '''

        # we ignore any passed CU.  In principle the cu info could be used to
        # determine which slots have been freed.  No need for that optimization
        # right now.  This will become interesting once reschedule becomes too
        # expensive.
        # FIXME: optimization

        cu = msg

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("before reschedule %s: %s", cu['uid'],
                            self.slot_status())

        # cycle through wait queue, and see if we get anything placed now.  We
        # cycle over a copy of the list, so that we can modify the list on the
        # fly,without locking the whole loop.  However, this is costly, too.
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

        # return True to keep the cb registered
        return True


# ------------------------------------------------------------------------------

