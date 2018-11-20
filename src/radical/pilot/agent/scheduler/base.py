
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__ = "MIT"


import logging
import pprint
import threading

import radical.utils as ru

from ... import utils as rpu
from ... import states as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
#
# 'enum' for RPs's pilot scheduler types
#
SCHEDULER_NAME_CONTINUOUS = "CONTINUOUS"
SCHEDULER_NAME_CONTINUOUS_FIFO = "CONTINUOUS_FIFO"
SCHEDULER_NAME_HOMBRE = "HOMBRE"
SCHEDULER_NAME_SCATTERED = "SCATTERED"
SCHEDULER_NAME_SPARK = "SPARK"
SCHEDULER_NAME_TORUS = "TORUS"
SCHEDULER_NAME_YARN = "YARN"


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
#   - notification management:
#     - the scheduler receives notifications about units which completed
#       execution, and whose resources can now be used again for other units,
#     - the above triggers an 'unschedule' (free resources) action and also a
#       `schedule` action (check waitlist if waiting units can now be placed).
#
#
# A scheduler implementation will derive from this base class, and overload the
# following three methods:
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
# gpus in the nodelist into which the unit can be placed.  It will mark those as
# `rpc.BUSY`, and attach the set of cores/gpus to the unit dictionary, as (here
# for system with 8 cores & 1 gpu per node):
#
#     unit = { ...
#       'cpu_processes'   : 4,
#       'cpu_process_type': 'mpi',
#       'cpu_threads'     : 2,
#       'gpu_processes    : 2,
#       'slots' :
#       {                 # [[node,   node_uid,   [cpu idx],        [gpu idx]]]
#         'nodes'         : [[node_1, node_uid_1, [[0, 2], [4, 6]], [[0]    ]],
#                            [node_2, node_uid_2, [[1, 3], [5, 7]], [[0]    ]]],
#         'cores_per_node': 8,
#         'gpus_per_node' : 1,
#         'lm_info'       : { ... }
#       }
#     }
#
# The `cpu idx` field is a list of sets, where in each set the first core is
# where an application process is places, while the other cores are reserved for
# that process' threads.  For GPUs we use the same structure, but GPU processes
# are currently all considered to be single-threaded.
#
# The respective launch method is expected to create processes on the set of
# cpus and gpus thus specified, (node_1, cores 0 and 4; node_2, cores 1 and 5).
# The other reserved cores are for the application to spawn threads on
# (`cpu_threads=2`).
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

# NOTE:  The set of profiler events generated by this component are:
#
#        schedule_try    : search for unit resources starts    (uid: uid)
#        schedule_fail   : search for unit resources failed    (uid: uid)
#        schedule_ok     : search for unit resources succeeded (uid: uid)
#        unschedule_start: unit resource freeing starts        (uid: uid)
#        unschedule_stop : unit resource freeing stops         (uid: uid)
#
#        See also:
#        https://github.com/radical-cybertools/radical.pilot/blob/feature/ \
#                           events/docs/source/events.md \
#                           #agentschedulingcomponent-component
#
# ==============================================================================
#
class AgentSchedulingComponent(rpu.Component):

    # --------------------------------------------------------------------------
    #
    # the deriving schedulers should in general have the following structure in
    # self.nodes:
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
        self._uid = ru.generate_id(cfg['owner'] + '.scheduling.%(counter)s',
                                   ru.ID_CUSTOM)

        self._uniform_waitpool = True   # TODO: move to cfg

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
        self.register_publisher(rpc.AGENT_SCHEDULE_PUBSUB)
        self.register_subscriber(rpc.AGENT_SCHEDULE_PUBSUB, self.schedule_cb)

        # The scheduler needs the LRMS information which have been collected
        # during agent startup.  We dig them out of the config at this point.
        #
        # NOTE: this information is insufficient for the torus scheduler!
        self._pilot_id = self._cfg['pilot_id']
        self._lrms_info = self._cfg['lrms_info']
        self._lrms_lm_info = self._cfg['lrms_info']['lm_info']
        self._lrms_node_list = self._cfg['lrms_info']['node_list']
        self._lrms_cores_per_node = self._cfg['lrms_info']['cores_per_node']
        self._lrms_gpus_per_node = self._cfg['lrms_info']['gpus_per_node']
        # Dict containing the size and path
        self._lrms_lfs_per_node = self._cfg['lrms_info']['lfs_per_node']   

        if not self._lrms_node_list:
            raise RuntimeError("LRMS %s didn't _configure node_list."
                              % self._lrms_info['name'])

        if self._lrms_cores_per_node is None:
            raise RuntimeError("LRMS %s didn't _configure cores_per_node."
                              % self._lrms_info['name'])

        if self._lrms_gpus_per_node is None:
            raise RuntimeError("LRMS %s didn't _configure gpus_per_node."
                              % self._lrms_info['name'])

        # create and initialize the wait pool
        self._wait_pool = list()             # pool of waiting units
        self._wait_lock = threading.RLock()  # look on the above pool
        self._slot_lock = threading.RLock()  # lock slot allocation/deallocation

        # initialize the node list to be used by the scheduler.  A scheduler
        # instance may decide to overwrite or extend this structure.

        self.nodes = []
        for node, node_uid in self._lrms_node_list:
            self.nodes.append({
                'name': node,
                'uid': node_uid,
                'cores': [rpc.FREE] * self._lrms_cores_per_node,
                'gpus': [rpc.FREE] * self._lrms_gpus_per_node,
                'lfs': self._lrms_lfs_per_node
            })

        # configure the scheduler instance
        self._configure()
        self._log.debug("slot status after  init      : %s",
                        self.slot_status())

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

        from .continuous_fifo import ContinuousFifo
        from .continuous import Continuous
        from .scattered import Scattered
        from .hombre import Hombre
        from .torus import Torus
        from .yarn import Yarn
        from .spark import Spark

        try:
            impl = {
                SCHEDULER_NAME_CONTINUOUS_FIFO: ContinuousFifo,
                SCHEDULER_NAME_CONTINUOUS: Continuous,
                SCHEDULER_NAME_SCATTERED: Scattered,
                SCHEDULER_NAME_HOMBRE: Hombre,
                SCHEDULER_NAME_TORUS: Torus,
                SCHEDULER_NAME_YARN: Yarn,
                SCHEDULER_NAME_SPARK: Spark
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
    #
    def _change_slot_states(self, slots, new_state):
        '''
        This function is used to update the state for a list of slots that
        have been allocated or deallocated.  For details on the data structure,
        see top of `base.py`.
        '''
        # This method needs to change if the DS changes.

        # for node_name, node_uid, cores, gpus in slots['nodes']:
        for nodes in slots['nodes']:

            # Find the entry in the the slots list

            # TODO: [Optimization] Assuming 'uid' is the ID of the node, it
            #       seems a bit wasteful to have to look at all of the nodes
            #       available for use if at most one node can have that uid.
            #       Maybe it would be worthwhile to simply keep a list of nodes
            #       that we would read, and keep a dictionary that maps the uid
            #       of the node to the location on the list?

            node = (n for n in self.nodes if n['uid'] == nodes['uid']).next()
            assert(node)

            # iterate over cores/gpus in the slot, and update state
            cores = nodes['core_map']
            for cslot in cores:
                for core in cslot:
                    node['cores'][core] = new_state

            gpus = nodes['gpu_map']
            for gslot in gpus:
                for gpu in gslot:
                    node['gpus'][gpu] = new_state

            if node['lfs']['path'] is not None:
                if new_state == rpc.BUSY:
                    node['lfs']['size'] -= nodes['lfs']['size']
                else:
                    node['lfs']['size'] += nodes['lfs']['size']

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
                if core == rpc.FREE:
                    ret += '-'
                else:
                    ret += '#'
            ret += ':'
            for gpu in node['gpus']:
                if gpu == rpc.FREE:
                    ret += '-'
                else:
                    ret += '#'
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
            if self._try_allocation(unit):
                # we could schedule the unit - advance its state, notify worls
                # about the state change, and push the unit out toward the next
                # component.
                self.advance(unit, rps.AGENT_EXECUTING_PENDING,
                             publish=True, push=True)
            else:
                # no resources available, put in wait queue
                with self._wait_lock:
                    self._wait_pool.append(unit)

    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, unit):
        """
        attempt to allocate cores/gpus for a specific unit.
        """

        # needs to be locked as we try to acquire slots here, but slots are
        # freed in a different thread.  But we keep the lock duration short...
        with self._slot_lock:

            self._prof.prof('schedule_try', uid=unit['uid'])
            unit['slots'] = self._allocate_slot(unit['description'])

        # the lock is freed here
        if not unit['slots']:

            # signal the unit remains unhandled (Fales signals that failure)
            self._prof.prof('schedule_fail', uid=unit['uid'])
            return False

        # got an allocation, we can go off and launch the process
        self._prof.prof('schedule_ok', uid=unit['uid'])

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("after  allocate   %s: %s", unit['uid'],
                            self.slot_status())
            self._log.debug("%s [%s/%s] : %s", unit['uid'],
                            unit['description']['cpu_processes'],
                            unit['description']['gpu_processes'],
                            pprint.pformat(unit['slots']))

        # True signals success
        return True

    # --------------------------------------------------------------------------
    #
    def _get_node_maps(self, cores, gpus, threads_per_proc):
        '''
        For a given set of cores and gpus, chunk them into sub-sets so that each
        sub-set can host one application process and all threads of that
        process.  Note that we currently consider all GPU applications to be
        single-threaded.

        example:
            cores  : [1, 2, 3, 4, 5, 6, 7, 8]
            gpus   : [1, 2]
            tpp    : 4
            result : [[1, 2, 3, 4], [5, 6, 7, 8]], [[1], [2]]

        For more details, see top level comment of `base.py`.
        '''

        core_map = list()
        gpu_map  = list()

        # make sure the core sets can host the requested number of threads
        assert(not len(cores) % threads_per_proc)
        n_procs =  len(cores) / threads_per_proc

        idx = 0
        for p in range(n_procs):
            p_map = list()
            for t in range(threads_per_proc):
                p_map.append(cores[idx])
                idx += 1
            core_map.append(p_map)

        if idx != len(cores):
            self._log.debug('%s -- %s -- %s -- %s',
                            idx, len(cores), cores, n_procs)
        assert(idx == len(cores))

        # gpu procs are considered single threaded right now (FIXME)
        for g in gpus:
            gpu_map.append([g])

        return core_map, gpu_map


    # --------------------------------------------------------------------------
    #
    def unschedule_cb(self, topic, msg):
        """
        release (for whatever reason) all slots allocated to this unit
        """

        unit = msg

        if not unit['slots']:
            # Nothing to do -- how come?
            self._log.error("cannot unschedule: %s (no slots)" % unit)
            return True

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("before unschedule %s: %s", unit['uid'],
                            self.slot_status())

        # needs to be locked as we try to release slots, but slots are acquired
        # in a different thread....
        with self._slot_lock:
            self._prof.prof('unschedule_start', uid=unit['uid'])
            self._release_slot(unit['slots'])
            self._prof.prof('unschedule_stop',  uid=unit['uid'])

        # notify the scheduling thread, ie. trigger an attempt to use the freed
        # slots for units waiting in the wait pool.
        self.publish(rpc.AGENT_SCHEDULE_PUBSUB, unit)

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("after  unschedule %s: %s", unit['uid'],
                            self.slot_status())

        # return True to keep the cb registered
        return True

    # --------------------------------------------------------------------------
    #
    def schedule_cb(self, topic, msg):
        '''
        This cb is triggered after a unit's resources became available again, so
        we can attempt to schedule units from the wait pool.
        '''

        # we ignore any passed unit.  In principle the unit info could be used to
        # determine which slots have been freed.  No need for that optimization
        # right now.  This will become interesting once schedule becomes too
        # expensive.
        # FIXME: optimization

        unit = msg

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("before schedule   %s: %s", unit['uid'],
                            self.slot_status())

        # cycle through wait queue, and see if we get anything placed now.  We
        # cycle over a copy of the list, so that we can modify the list on the
        # fly,without locking the whole loop.  However, this is costly, too.
        for unit in self._wait_pool[:]:

            if self._try_allocation(unit):

                # allocated unit -- advance it
                self.advance(unit, rps.AGENT_EXECUTING_PENDING, publish=True, push=True)

                # remove it from the wait queue
                with self._wait_lock:
                    self._wait_pool.remove(unit)

            else:
                # Break out of this loop if we didn't manage to schedule a task
                # FIXME: this assumes that no smaller or otherwise more suitable
                #        CUs come after this one - which is naive, ie. wrong.
                # NOTE:  This assumption does indeed break for the fifo
                #        scheduler, so we disable this now for non-uniform cases
                if self._uniform_waitpool:
                    break

        # return True to keep the cb registered
        return True


# ------------------------------------------------------------------------------
