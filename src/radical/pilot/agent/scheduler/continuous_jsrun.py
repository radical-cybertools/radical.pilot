
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import pprint

import math as m

import radical.utils as ru

from ...   import constants as rpc
from .base import AgentSchedulingComponent


# ------------------------------------------------------------------------------
#
# This is a continuous scheduler with awareness of a node's file-storage and
# memory capabilities.  The scheduler respects colocate tagging (place task on
# same node as other tasks with the same tag).
#
#
# Continuous:
#
# The scheduler attempts to find continuous stretches of nodes to place
# a multinode-task.
#
#
# Local Storage:
#
# The storage availability will be obtained from the rm_node_list and assigned
# to the node list of the class. The lfs requirement will be obtained from the
# td in the alloc_nompi and alloc_mpi methods. Using the availability and
# requirement, the _find_resources method will return the core and gpu ids.
#
# Expected DS of the nodelist
# self.nodes = [{
#                   'name'     : 'aa',
#                   'index'    : 0,
#                   'cores'    : [0, 1, 2, 3, 4, 5, 6, 7],
#                   'gpus'     : [0, 1, 2],
#                   'lfs'      : 128,
#                   'mem'      : 256
#               },
#               {
#                   'name'     : 'bb',
#                   'index'    : 1,
#                   'cores'    : [0, 1, 2, 3, 4, 5, 6, 7],
#                   'gpus'     : [0, 1, 2],
#                   'lfs'      : 128,
#                   'mem'      : 256,
#                },
#                ...
#              ]
#
# lfs storage (size) and memory is specified in MByte.  The scheduler assumes
# that both are freed when the task finishes.
#
#
# Task Tagging:
#
# The scheduler attempts to schedule tasks with the same colocate tag onto the
# same node, so that the task can reuse the previous task's data.  This assumes
# that storage is not freed when the tasks finishes.
#
# FIXME: the alert reader will realize a discrepancy in the above set of
#        assumptions.
#
# ------------------------------------------------------------------------------
#
class ContinuousJsrun(AgentSchedulingComponent):
    '''
    The Continuous scheduler attempts to place threads and processes of
    a tasks onto nodes in the cluster.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentSchedulingComponent.__init__(self, cfg, session)

        self._colo_history = dict()
        self._tagged_nodes = set()
        self._scattered    = None
        self._node_offset  = 0



    # --------------------------------------------------------------------------
    #
    def _configure(self):
        '''
        Configure this scheduler instance

        * scattered:
          This is the continuous scheduler, because it attempts to allocate
          a *continuous* set of cores/nodes for a task.  It does, however,
          also allow to scatter the allocation over discontinuous nodes if
          this option is set.  This implementation is not optimized for the
          scattered mode!  The default is 'False'.
        '''

        self._scattered = self.session.rcfg.get('scattered', False)


    # --------------------------------------------------------------------------
    #
    def _iterate_nodes(self):
        '''
        The scheduler iterates through the node list for each task placement.
        However, we want to avoid starting from node zero every time as tasks
        have likely placed on that node previously - instead, we in general want
        to pick off where the last task placement succeeded.  This iterator is
        preserving that state.

        Note that the first index is yielded twice, so that the respective
        node can function as first and last node in an allocation.
        '''

        iterator_count = 0

        while iterator_count < len(self.nodes):
            yield self.nodes[self._node_offset]
            iterator_count    += 1
            self._node_offset += 1
            self._node_offset  = self._node_offset % len(self.nodes)


    # --------------------------------------------------------------------------
    #
    def unschedule_task(self, tasks):
        '''
        This method is called when previously acquired resources are not needed
        anymore.  `slots` are the resource slots as previously returned by
        `schedule_task()`.
        '''

        # reflect the request in the nodelist state (set to `FREE`)
        for task in ru.as_list(tasks):
            self._change_slot_states(task['slots'], rpc.FREE)


    # --------------------------------------------------------------------------
    #
    def _find_resources(self, node, n_slots, ranks_per_slot, cores_per_slot,
                        gpus_per_slot, lfs_per_slot, mem_per_slot, partial):
        '''
        Find up to the requested number of slots, where each slot features the
        respective `x_per_slot` resources.  This call will return a list of
        slots of the following structure:

            {
                'node_name' : 'node_name',
                'node_index': '1',
                'core_map'  : [[1, 2, 4, 5], [6, 7, 8, 9]],
                'gpu_map'   : [[1, 3], [1, 3]],
                'lfs'       : 1234,
                'mem'       : 4321
            }

        The call will *not* change the allocation status of the node, atomicity
        must be guaranteed by the caller.

        We don't care about continuity within a single node - cores `[1, 5]` are
        assumed to be as close together as cores `[1, 2]`.

        When `partial` is set, the method CAN return less than `n_slots` number
        of slots - otherwise, the method returns the requested number of slots
        or `None`.

        FIXME: SMT handling: we should assume that hardware threads of the same
               physical core cannot host different executables, so HW threads
               can only account for thread placement, not process placement.
               This might best be realized by internally handling SMT as minimal
               thread count and using physical core IDs for process placement?
        '''

      # self._log.debug('find on %s: %s * [%s, %s]', node['uid'], )

        # check if the node can host the request
        free_cores = node['cores'].count(rpc.FREE)
        free_gpus  = node['gpus'].count(rpc.FREE)
        free_lfs   = node['lfs']
        free_mem   = node['mem']

        # check how many slots we can serve, at most
        alc_slots = 1
        if cores_per_slot:
            alc_slots = int(m.floor(free_cores / cores_per_slot))

        if gpus_per_slot:
            alc_slots = min(alc_slots, int(m.floor(free_gpus / gpus_per_slot)))

        if lfs_per_slot:
            alc_slots = min(alc_slots, int(m.floor(free_lfs / lfs_per_slot)))

        if mem_per_slot:
            alc_slots = min(alc_slots, int(m.floor(free_mem / mem_per_slot)))

        # is this enough?
        if not alc_slots:
            return None

        if not partial:
            if alc_slots < n_slots:
                return None

        # find at most `n_slots`
        alc_slots = min(alc_slots, n_slots)

        # we should be able to host the slots - dig out the precise resources
        slots      = list()
        node_index = node['index']
        node_name  = node['name']

        core_idx   = 0
        gpu_idx    = 0

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

            cores_per_rank = cores_per_slot // ranks_per_slot
            # create number of lists (equal to `ranks_per_slot`) with
            # cores indices (i.e., cores per rank)
            core_map = [cores[i:i + cores_per_rank]
                        for i in range(0, len(cores), cores_per_rank)]
            # gpus per rank are the same within the slot
            gpu_map  = [gpus] * len(core_map)

            slots.append({'node_name' : node_name,
                          'node_index': node_index,
                          'cores'     : core_map,
                          'gpus'      : gpu_map,
                          'lfs'       : lfs_per_slot,
                          'mem'       : mem_per_slot})

        # consistency check
        assert (len(slots) == n_slots) or (len(slots) and partial)

        return slots


    # --------------------------------------------------------------------------
    #
    def _change_slot_states(self, slots, new_state):
        '''
        This function is used to update the state for a list of slots that
        have been allocated or deallocated.  For details on the data structure,
        see top of `base.py`.
        '''

        # for node_name, node_index, cores, gpus in slots['ranks']:
        for slot in slots:

            # Find the entry in the slots list

            # TODO: [Optimization] Assuming 'node_index' is the ID of the node,
            #       it seems a bit wasteful to have to look at all of the nodes
            #       available for use if at most one node can have that uid.
            #       Maybe it would be worthwhile to simply keep a list of nodes
            #       that we would read, and keep a dictionary that maps the uid
            #       of the node to the location on the list?

            node = None
            node_found = False
            for node in self.nodes:
                if node['index'] == slot['node_index']:
                    node_found = True
                    break

            if not node_found:
                raise RuntimeError('inconsistent node information')

            # iterate over cores/gpus in the slot, and update state
            for core_map in slot['cores']:
                for core in core_map:
                    node['cores'][core] = new_state

            for gpu_map in slot['gpus']:
                for gpu in gpu_map:
                    node['gpus'][gpu] = new_state

            if slot['lfs']:
                if new_state == rpc.BUSY:
                    node['lfs'] -= slot['lfs']
                else:
                    node['lfs'] += slot['lfs']

            if slot['mem']:
                if new_state == rpc.BUSY:
                    node['mem'] -= slot['mem']
                else:
                    node['mem'] += slot['mem']


    # --------------------------------------------------------------------------
    #
    #
    def schedule_task(self, task):
        '''
        Find an available set of slots, potentially across node boundaries (in
        the MPI case).

        A `slot` is here considered the amount of resources required by a single
        MPI rank.  Those resources need to be available on a single node - but
        slots can be distributed across multiple nodes.  Resources for non-MPI
        tasks will always need to be placed on a single node.

        By default, we only allow for partial allocations on the first and last
        node - but all intermediate nodes MUST be completely used (this is the
        'CONTINUOUS' scheduler after all).

        If the scheduler is configured with `scattered=True`, then that
        constraint is relaxed, and any set of slots (be it continuous across
        nodes or not) is accepted as valid allocation.

        No matter the mode, we always make sure that we allocate slots in chunks
        of cores, gpus, lfs and mem required per process - otherwise the
        application processes would not be able to acquire the requested
        resources on the respective node.
        '''

        self._log.debug_3('find_resources %s', task['uid'])

        td  = task['description']
        mpi = bool(td['ranks'] > 1)

        cores_per_node = self._rm.info.cores_per_node
        gpus_per_node  = self._rm.info.gpus_per_node
        lfs_per_node   = self._rm.info.lfs_per_node
        mem_per_node   = self._rm.info.mem_per_node

        cores_per_slot = td['cores_per_rank']
        gpus_per_slot  = td['gpus_per_rank']
        lfs_per_slot   = td['lfs_per_rank']
        mem_per_slot   = td['mem_per_rank']

        # make sure that processes are at least single-threaded
        if not cores_per_slot:
            cores_per_slot = 1

        # check if there is a GPU sharing
        if gpus_per_slot and not gpus_per_slot.is_integer():
            gpus           = m.ceil(td['ranks'] * gpus_per_slot)
            # find the greatest common divisor
            req_slots      = m.gcd(td['ranks'], gpus)
            ranks_per_slot = td['ranks'] // req_slots
            gpus_per_slot  = gpus        // req_slots

            cores_per_slot *= ranks_per_slot
            lfs_per_slot   *= ranks_per_slot
            mem_per_slot   *= ranks_per_slot

        else:
            req_slots      = td['ranks']
            ranks_per_slot = 1
            gpus_per_slot  = int(gpus_per_slot)

        self._log.debug_7('req : %s %s %s %s %s %s',
                          req_slots, ranks_per_slot, cores_per_slot,
                          gpus_per_slot, lfs_per_slot, mem_per_slot)

        # First and last nodes can be a partial allocation - all other nodes
        # can only be partial when `scattered` is set.
        #
        # Iterate over all nodes until we find something. Check if it fits the
        # allocation mode and sequence.  If not, start over with the next node.
        # If it matches, add the slots found and continue to next node.
        #
        # FIXME: persistent node index

        # we always fail when too many threads are requested
        assert cores_per_slot <= cores_per_node, \
               'too many threads per proc %s' % cores_per_slot
        assert gpus_per_slot  <= gpus_per_node, \
               'too many gpus    per proc %s' % gpus_per_slot
        assert lfs_per_slot   <= lfs_per_node, \
               'too much lfs     per proc %s' % lfs_per_slot
        assert mem_per_slot   <= mem_per_node, \
               'too much mem     per proc %s' % mem_per_slot

        # check what resource type limits teh number of slots per node
        tmp = list()
        slots_per_node = int(m.floor(cores_per_node / cores_per_slot))
        tmp.append([cores_per_node, cores_per_slot, slots_per_node])

        if gpus_per_slot:
            slots_per_node = min(slots_per_node,
                                 int(m.floor(gpus_per_node / gpus_per_slot)))
        tmp.append([gpus_per_node, gpus_per_slot, slots_per_node])

        if lfs_per_slot:
            slots_per_node = min(slots_per_node,
                                 int(m.floor(lfs_per_node / lfs_per_slot)))
        tmp.append([lfs_per_node, lfs_per_slot, slots_per_node])

        if mem_per_slot:
            slots_per_node = min(slots_per_node,
                                 int(m.floor(mem_per_node / mem_per_slot)))
        tmp.append([mem_per_node, mem_per_slot, slots_per_node])

        if not mpi and req_slots > slots_per_node:
            raise ValueError('non-mpi task does not fit on a single node:'
                    '%s * %s:%s > %s:%s -- %s > %s [%s %s] %s' % (req_slots,
                    cores_per_slot, gpus_per_slot,
                    cores_per_node, gpus_per_node, req_slots,
                    slots_per_node, cores_per_slot, gpus_per_slot, tmp))

        # set conditions to find the first matching node
        is_first = True
        is_last  = False
        colo_tag = td['tags'].get('colocate')

        if colo_tag is not None:
            colo_tag = str(colo_tag)

        # in case of PRTE LM: the `slots` attribute may have a partition ID set
        partition_id = td.get('partition', 0)
        if self._partition_ids:
            if partition_id not in self._partition_ids:
                raise ValueError('partition id (%d) out of range'
                                 % partition_id)

            # partition id becomes a part of a co-locate tag
            colo_tag = str(partition_id) + ('' if not colo_tag else '_%s' % colo_tag)
            if colo_tag not in self._colo_history:
                self._colo_history[colo_tag] = self._partition_ids[partition_id]
        task_partition_id = None

        # what remains to be allocated?  all of it right now.
        alc_slots = list()
        rem_slots = req_slots

        # start the search
        for node in self._iterate_nodes():

            node_index = node['index']
            node_name  = node['name']

            self._log.debug_7('next %d : %s', node_index, node_name)
            self._log.debug_7('req1: %s = %s + %s', req_slots, rem_slots,
                                                  len(alc_slots))

            # Check if a task is tagged to use this node.  This means we check
            #   - if a "colocate" tag exists
            #   - if a "colocate" tag has been used before
            #   - if the previous use included this node
            # If a tag exists, continue to consider this node if the tag was
            # used for this node - else continue to the next node.
            if colo_tag is not None:
                if colo_tag in self._colo_history:
                    if node_index not in self._colo_history[colo_tag]:
                        continue
                # for a new tag check that nodes were not used for previous tags
                else:
                    # `exclusive` -> not to share nodes between different tags
                    is_exclusive = td['tags'].get('exclusive', False)
                    if is_exclusive and node_index in self._tagged_nodes:
                        if len(self.nodes) > len(self._tagged_nodes):
                            continue
                        self._log.warn('not enough nodes for exclusive tags, ' +
                                       'switched "exclusive" flag to "False"')

            node_partition_id = None
            if self._partition_ids:
                # nodes assigned to the task should be from the same partition
                # FIXME: handle the case when unit (MPI task) would require
                #        more nodes than the amount available per partition
                # FIXME: needs fixing when using the scheduler
                # _skip_node = True
                # for plabel, p_node_indexs in self._partitions.items():
                #     if node_index in p_node_indexs:
                #         if task_partition_id in [None, plabel]:
                #             node_partition_id = plabel
                #             _skip_node = False
                #         break
                # if _skip_node:
                #     continue
                pass

            # if only a small set of cores/gpus remains unallocated (i.e., less
            # than node size), we are in fact looking for the last node.  Note
            # that this can also be the first node, for small tasks.
            if rem_slots < slots_per_node:
                is_last = True

            if not mpi:
                # non-mpi tasks are never partially allocated
                partial = False

            elif is_first or self._scattered or is_last:
                # we allow partial nodes on the first and last node,
                # and on any node if a 'scattered' allocation is requested.
                partial = True

            else:
                partial = False

            # now we know how many slots we still need at this point - but
            # we only search up to node-size on this node.  Duh!
            n_slots = min(rem_slots, slots_per_node)
            self._log.debug_7('find %s slots', n_slots)

            # under the constraints so derived, check what we find on this node
            new_slots = self._find_resources(node           = node,
                                             n_slots        = n_slots,
                                             ranks_per_slot = ranks_per_slot,
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
                    alc_slots = list()
                    rem_slots = req_slots
                    is_first  = True
                    is_last   = False

                # try next node
                continue

            if node_partition_id is not None and task_partition_id is None:
                task_partition_id = node_partition_id

            # this node got a match, store away the found slots and continue
            # search for remaining ones
            rem_slots -= len(new_slots)
            alc_slots.extend(new_slots)

            self._log.debug_7('new slots: %s', pprint.pformat(new_slots))
            self._log.debug_7('req2: %s = %s + %s <> %s', req_slots, rem_slots,
                                                  len(new_slots), len(alc_slots))

            # we are young only once.  kinda...
            is_first = False

            # or maybe don't continue the search if we have in fact enough!
            if rem_slots == 0:
                break

        # if we did not find enough, there is not much we can do at this point
        if  rem_slots > 0:
            return None, None  # signal failure


        # if tag `colocate` was provided, then corresponding nodes should be
        # stored in the tag history (if partition nodes were kept under this
        # key before then it will be overwritten)
        if colo_tag is not None and colo_tag != str(partition_id):
            self._colo_history[colo_tag] = [slot['node_index']
                                            for slot in alc_slots]
            self._tagged_nodes.update(self._colo_history[colo_tag])

        # this should be nicely filled out now - return
        return alc_slots, task_partition_id


# ------------------------------------------------------------------------------

