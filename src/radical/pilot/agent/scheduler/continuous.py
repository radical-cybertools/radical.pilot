
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__ = "MIT"

import pprint

import math as m

from ...   import constants as rpc
from .base import AgentSchedulingComponent


# ------------------------------------------------------------------------------
#
# This is a continuous scheduler with awareness of a node's file-storage and
# memory capabilities.  The scheduler respects node tagging (place unit on same
# node as other units with the same tag).
#
#
# Continuous:
#
# The scheduler attempts to find continuous stretches of nodes to place
# a multinode-unit.
#
#
# Local Storage:
#
# The storage availability will be obtained from the rm_node_list and assigned
# to the node list of the class. The lfs requirement will be obtained from the
# cud in the alloc_nompi and alloc_mpi methods. Using the availability and
# requirement, the _find_resources method will return the core and gpu ids.
#
# Expected DS of the nodelist
# self.nodes = [{
#                   'name'    : 'aa',
#                   'uid'     : 'node.0000',
#                   'cores'   : [0, 1, 2, 3, 4, 5, 6, 7],
#                   'gpus'    : [0,1, 2],
#                   'lfs'     : 128,
#                   'mem'     : 256
#               },
#               {
#                   'name'    : 'bb',
#                   'uid'     : 'node.0001',
#                   'cores'   : [0, 1, 2, 3, 4, 5, 6, 7],
#                   'gpus'    : [0,1, 2],
#                   'lfs'     : 256,
#                   'mem'     : 256,
#                },
#                ...
#              ]
#
# lfs storage and memory is specified in MByte.  The scheduler assumes that
# both are freed when the unit finishes.
#
#
# Unit Tagging:
#
# The scheduler attempts to schedule units with the same tag onto the same node,
# so that the unit can reuse the previous unit's data.  This assumes that
# storage is not freed when the units finishes.
#
# FIXME: the alert reader will realize a discrepancy in the above set of
#        assumptions.
#
# ------------------------------------------------------------------------------
#
class Continuous(AgentSchedulingComponent):
    '''
    The Continuous scheduler attempts to place threads and processes of
    a compute units onto nodes in the cluster.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentSchedulingComponent.__init__(self, cfg, session)

        self._tag_history   = dict()
        self._scattered     = None
        self._node_offset   = 0


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # * scattered:
        #   This is the continuous scheduler, because it attempts to allocate
        #   a *continuous* set of cores/nodes for a unit.  It does, however,
        #   also allow to scatter the allocation over discontinuous nodes if
        #   this option is set.  This implementation is not optimized for the
        #   scattered mode!  The default is 'False'.
        #
        self._scattered = self._cfg.get('scattered', False)

        self.nodes = []
        for node, node_uid in self._rm_node_list:

            node_entry = {'name'   : node,
                          'uid'    : node_uid,
                          'cores'  : [rpc.FREE] * self._rm_cores_per_node,
                          'gpus'   : [rpc.FREE] * self._rm_gpus_per_node,
                          'lfs'    :              self._rm_lfs_per_node,
                          'mem'    :              self._rm_mem_per_node}

            # summit
            if  self._rm_cores_per_node > 40 and \
                self._cfg['task_launch_method'] == 'JSRUN':

                # Summit cannot address the last core of the second socket at
                # the moment, so we mark it as `DOWN` and the scheduler skips
                # it.  We need to check the SMT setting to make sure the right
                # logical cores are marked.  The error we see on those cores is:
                # "ERF error: 1+ cpus are not available"
                #
                # This is related to the known issue listed on
                # https://www.olcf.ornl.gov/for-users/system-user-guides \
                #                          /summit/summit-user-guide/
                #
                # "jsrun explicit resource file (ERF) allocates incorrect
                # resources"
                #
                smt = self._rm_info.get('smt', 1)

                # only socket `1` is affected at the moment
              # for s in [0, 1]:
                for s in [1]:
                    for i in range(smt):
                        idx = s * 21 * smt + i
                        node_entry['cores'][idx] = rpc.DOWN

            self.nodes.append(node_entry)

        if self._rm_cores_per_node > 40 and \
           self._cfg['task_launch_method'] == 'JSRUN':
            self._rm_cores_per_node -= 1


    # --------------------------------------------------------------------------
    #
    def _iterate_nodes(self):
        # note that the first index is yielded twice, so that the respecitve
        # node can function as first and last node in an allocation.

        iterator_count = 0

        while iterator_count < len(self.nodes):
            yield self.nodes[self._node_offset]
            iterator_count    += 1
            self._node_offset += 1
            self._node_offset  = self._node_offset % len(self.nodes)


    # --------------------------------------------------------------------------
    #
    def unschedule_unit(self, unit):
        '''
        This method is called when previously aquired resources are not needed
        anymore.  `slots` are the resource slots as previously returned by
        `schedule_unit()`.
        '''

        # reflect the request in the nodelist state (set to `FREE`)
        self._change_slot_states(unit['slots'], rpc.FREE)


    # --------------------------------------------------------------------------
    #
    def _find_resources(self, node, find_slots, cores_per_slot, gpus_per_slot,
                              lfs_per_slot, mem_per_slot, partial):
        '''
        Find up to the requested number of slots, where each slot features the
        respective `x_per_slot` resources.  This call will return a list of
        slots of the following structure:

            {
                'node_name': 'node_1',
                'node_uid' : 'node_1',
                'cores'    : [1, 2, 4, 5],
                'gpus'     : [1, 3],
                'lfs'      : 1234,
                'mem'      : 4321
            }

        The call will *not* change the allocation status of the node, atomicity
        must be guaranteed by the caller.

        We don't care about continuity within a single node - cores `[1, 5]` are
        assumed to be as close together as cores `[1, 2]`.

        When `partial` is set, the method CAN return less than `find_slots`
        number of slots - otherwise, the method returns the requested number of
        slots or `None`.

        FIXME: SMT handling: we should assume that hardware threads of the same
               physical core cannot host different executables, so HW threads
               can only account for thread placement, not process placement.
               This might best be realized by internally handling SMT as minimal
               thread count and using physical core IDs for process placement?
        '''

        # check if the node can host the request
        free_cores = node['cores'].count(rpc.FREE)
        free_gpus  = node['gpus'].count(rpc.FREE)
        free_lfs   = node['lfs']['size']
        free_mem   = node['mem']

        # check how many slots we can serve, at most
        alc_slots = 1
        if cores_per_slot:
            alc_slots = int(m.floor(free_cores / cores_per_slot))

        if gpus_per_slot:
            alc_slots = min(alc_slots, int(m.floor(free_gpus / gpus_per_slot )))

        if lfs_per_slot:
            alc_slots = min(alc_slots, int(m.floor(free_lfs / lfs_per_slot )))

        if mem_per_slot:
            alc_slots = min(alc_slots, int(m.floor(free_mem / mem_per_slot )))

        # is this enough?
        if not alc_slots:
            return None

        if not partial:
            if alc_slots < find_slots:
                return None

        # find at most `find_slots`
        alc_slots = min(alc_slots, find_slots)

        # we should be able to host the slots - dig out the precise resources
        slots     = list()
        node_uid  = node['uid']
        node_name = node['name']

        core_idx  = 0
        gpu_idx   = 0

        for _ in range(alc_slots):

            cores = list()
            gpus  = list()

            while len(cores) < cores_per_slot:

                if node['cores'][core_idx] == rpc.FREE:
                    cores.append(core_idx)
                core_idx += 1

            while len(gpus) < gpus_per_slot:

                if node['gpus'][gpu_idx] == rpc.FREE:
                    gpus.append(gpu_idx)
                gpu_idx += 1

            core_map = [cores]
            gpu_map  = [[gpu] for gpu in gpus]

            slots.append({'uid'     : node_uid,
                          'name'    : node_name,
                          'core_map': core_map,
                          'gpu_map' : gpu_map,
                          'lfs'     : {'size': lfs_per_slot,
                                       'path': self._rm_lfs_per_node['path']},
                          'mem'     : mem_per_slot})

        # consistency check
        assert((len(slots) == find_slots) or (len(slots) and partial))

        return slots


    # --------------------------------------------------------------------------
    #
    #
    def schedule_unit(self, unit):
        '''
        Find an available set of slots, potentially across node boundaries (in
        the MPI case).  By default, we only allow for partial allocations on the
        first and last node - but all intermediate nodes MUST be completely used
        (this is the 'CONTINUOUS' scheduler after all).

        If the scheduler is configured with `scattered=True`, then that
        constraint is relaxed, and any set of slots (be it continuous across
        nodes or not) is accepted as valid allocation.

        No matter the mode, we always make sure that we allocate slots in chunks
        of cores, gpus, lfs and mem required per process - otherwise the
        application processes would not be able to acquire the requested
        resources on the respective node.

        Contrary to the documentation, this scheduler interprets `gpu_processes`
        as number of GPUs that need to be available to each process, i.e., as
        `gpus_per_process'.

        Note that all resources for non-MPI tasks will always need to be placed
        on a single node.
        '''

        self._log.debug('find_resources %s', unit['uid'])

        cud = unit['description']
        mpi = bool('mpi' in cud['cpu_process_type'].lower())

        # dig out the allocation request details
        req_slots      = cud['cpu_processes']
        cores_per_slot = cud['cpu_threads']
        gpus_per_slot  = cud['gpu_processes']
        lfs_per_slot   = cud['lfs_per_process']
        mem_per_slot   = cud['mem_per_process']

        # make sure that processes are at least single-threaded
        if not cores_per_slot:
            cores_per_slot = 1

        self._log.debug('req : %s %s %s %s %s', req_slots, cores_per_slot,
                        gpus_per_slot, lfs_per_slot, mem_per_slot)

        # First and last nodes can be a partial allocation - all other nodes
        # can only be partial when `scattered` is set.
        #
        # Iterate over all nodes until we find something. Check if it fits the
        # allocation mode and sequence.  If not, start over with the next node.
        # If it matches, add the slots found and continue to next node.
        #
        # FIXME: persistent node index

        cores_per_node = self._rm_cores_per_node
        gpus_per_node  = self._rm_gpus_per_node
        lfs_per_node   = self._rm_lfs_per_node['size']
        mem_per_node   = self._rm_mem_per_node

        # we always fail when too many threads are requested
        assert(cores_per_slot <= cores_per_node), 'too many threads per proc'
        assert(gpus_per_slot  <= gpus_per_node),  'too many gpus    per proc'
        assert(lfs_per_slot   <= lfs_per_node),   'too much lfs     per proc'
        assert(mem_per_slot   <= mem_per_node),   'too much mem     per proc'

        # check what resource type limits teh number of slots per node
        slots_per_node = int(m.floor(cores_per_node / cores_per_slot))

        if gpus_per_slot:
            slots_per_node = min(slots_per_node,
                                 int(m.floor(gpus_per_node / gpus_per_slot )))

        if lfs_per_slot:
            slots_per_node = min(slots_per_node,
                                 int(m.floor(lfs_per_node / lfs_per_slot)))

        if mem_per_slot:
            slots_per_node = min(slots_per_node,
                                 int(m.floor(mem_per_node / mem_per_slot)))

        if not mpi and req_slots > slots_per_node:
            raise ValueError('non-mpi task does not fit on a single node')

        # set conditions to find the first matching node
        is_first = True
        is_last  = False
        tag      = cud.get('tag')

        # what remains to be allocated?  all of it right now.
        alc_slots = list()
        rem_slots = req_slots

        # start the search
        for node in self._iterate_nodes():

            node_uid  = node['uid']
          # node_name = node['name']

          # self._log.debug('next %s : %s', node_uid, node_name)
          # self._log.debug('req1: %s = %s + %s', req_slots, rem_slots,
          #                                       len(alc_slots))

            # Check if a unit is tagged to use this node.  This means we check
            #   - if a tag exists
            #   - if the tag has been used before
            #   - if the previous use included this node
            # If a tag exists, continue to consider this node if the tag was
            # used for this node - else continune to the next node.
            if tag:
                if tag in self._tag_history:
                    if node_uid not in self._tag_history[tag]:
                        continue

            # if only a small set of cores/gpus remains unallocated (ie. less
            # than node size), we are in fact looking for the last node.  Note
            # that this can also be the first node, for small units.
            if  rem_slots < slots_per_node:
                is_last = True

            # we allow partial nodes on the first and last node, and on any
            # node if a 'scattered' allocation is requested.
            if is_first or self._scattered or is_last:
                partial = True
            else:
                partial = False

            # but also, non-mpi tasks are never partially allocated
            if not mpi:
                partial = False

            # now we know how many slots we still need at this point - but
            # we only search up to node-size on this node.  Duh!
            find_slots = min(rem_slots, slots_per_node)
          # self._log.debug('find: %s', find_slots)

            # under the constraints so derived, check what we find on this node
            new_slots = self._find_resources(node           = node,
                                             find_slots     = find_slots,
                                             cores_per_slot = cores_per_slot,
                                             gpus_per_slot  = gpus_per_slot,
                                             lfs_per_slot   = lfs_per_slot,
                                             mem_per_slot   = mem_per_slot,
                                             partial        = partial)

            if not new_slots:

                # this was not a match. If we are in  'scattered' mode, we just
                # ignore this node.  Otherwise we have to restart the search
                # (continuity is broken)
                if not self._scattered:
                    alc_slots       = list()
                    rem_slots       = req_slots
                    is_first        = True
                    is_last         = False

                # try next node
                continue

            # this node got a match, store away the found slots and continue
            # search for remaining ones
            rem_slots -= len(new_slots)
            alc_slots.extend(new_slots)

          # self._log.debug('new slots: %s', pprint.pformat(new_slots))
          # self._log.debug('req2: %s = %s + %s <> %s', req_slots, rem_slots,
          #                                       len(new_slots), len(alc_slots))

            # we are young only once.  kinda...
            is_first = False

            # or maybe don't continue the search if we have in fact enough!
            if rem_slots == 0:
                break

        # if we did not find enough, there is not much we can do at this point
        if  rem_slots > 0:
            return None  # signal failure

        slots = {'nodes'         : alc_slots,
                 'cores_per_node': self._rm_cores_per_node,
                 'gpus_per_node' : self._rm_gpus_per_node,
                 'lfs_per_node'  : self._rm_lfs_per_node,
                 'mem_per_node'  : self._rm_mem_per_node,
                 'lm_info'       : self._rm_lm_info,
                }


        # allocation worked!  If the unit was tagged, store the node IDs for
        # this tag, so that later units can reuse that information
        tag = unit['description'].get('tag')
        if tag:
            self._tag_history[tag] = [node['uid'] for node in slots['nodes']]

        # this should be nicely filled out now - return
        return slots


# ------------------------------------------------------------------------------

