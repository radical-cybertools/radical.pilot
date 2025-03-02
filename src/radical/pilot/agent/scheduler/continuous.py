
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import pprint

import math as m

import radical.utils as ru

from .base              import AgentSchedulingComponent
from ...                import constants as rpc
from ...resource_config import RO


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
class Continuous(AgentSchedulingComponent):
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
          scattered mode!  The default is 'True'.
        '''

        self._scattered = self.session.rcfg.get('scattered', True)


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
    def _find_resources(self, node, n_slots, cores_per_slot,
                        gpus_per_slot, lfs_per_slot, mem_per_slot, partial):
        '''
        Find up to the requested number of slots, where each slot features the
        respective `x_per_slot` resources.  This call will return a list of
        slots of the following structure:

            {
                'node_name'  : 'node_name',
                'node_index' : 1,
                'cores'      : [{'index'     : 1,
                                 'occupation': 1.0},
                                {'index'     : 2,
                                 'occupation': 1.0},
                                {'index'     : 4,
                                 'occupation': 1.0},
                                {'index'     : 5,
                                 'occupation': 1.0}],
                'gpus'       : [{'index'     : 1,
                                 'occupation': 1.0},
                                {'index'     : 2,
                                 'occupation': 1.0}],
                'lfs'        : 1234,
                'mem'        : 4321
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

        slots = list()

        # find at most `n_slots`
        loop_core_idx = 0
        loop_gpu_idx  = 0
        while len(slots) < n_slots:

            node_idx  = node['index']
            node_name = node['name']

            self._log.debug_9('find resources on %s:%d', node_name, node_idx)
            self._log.debug_9('node: %s', pprint.pformat(node))
            self._log.debug_9('cps : %s', cores_per_slot)

            slot  = {'node_name' : node_name,
                     'node_index': node_idx,
                     'cores'     : list(),
                     'gpus'      : list(),
                     'lfs'       : lfs_per_slot,
                     'mem'       : mem_per_slot}

            for core_idx,core in enumerate(node['cores'][loop_core_idx:],
                                                         loop_core_idx):
                if core == rpc.FREE:
                    slot['cores'].append(RO(index=core_idx,
                                            occupation=rpc.BUSY))

                if len(slot['cores']) == cores_per_slot:
                    break

            loop_core_idx = core_idx + 1

            if len(slot['cores']) < cores_per_slot:
                self._log.debug_9('not enough cores on %s', node_name)
                break

            # gpus can be shared, so we need proper resource tracking.  If
            # a slot requires one or more GPUs, GPU sharing is disabled.
            if gpus_per_slot >= 1.0:

                tmp = int(gpus_per_slot)
                if tmp != gpus_per_slot:
                    raise ValueError('cannot share GPUs>1')
                gpus_per_slot = tmp

                for gpu_idx,gpu in enumerate(node['gpus'][loop_gpu_idx:],
                                                          loop_gpu_idx):

                    if gpu == rpc.FREE:
                        slot['gpus'].append(RO(index=gpu_idx,
                                               occupation=rpc.BUSY))

                    if len(slot['gpus']) == gpus_per_slot:
                        break

                loop_gpu_idx = gpu_idx + 1

                if len(slot['gpus']) < gpus_per_slot:
                    self._log.debug_9('not enough gpus on %s (1)', node_name)
                    break

            elif gpus_per_slot > 0.0:

                # find a GPU which has sufficient space left
                for gpu_idx,gpu_occ in enumerate(node['gpus'][loop_gpu_idx:],
                                                              loop_gpu_idx):

                    if gpus_per_slot <= rpc.BUSY - gpu_occ:
                        slot['gpus'].append(RO(index=gpu_idx,
                                               occupation=gpus_per_slot))
                        break
                    else:
                        loop_gpu_idx = gpu_idx + 1

                if len(slot['gpus']) < 1:
                    self._log.debug_9('not enough gpus on %s (2)', node_name)
                    break

            self._log.debug_9('found resources on %s: %s', node_name, slot)

            slots.append(slot)

        self._log.debug_9('found resources on %s', node_name)
        self._log.debug_9(pprint.pformat(slots))

        if not partial and len(slots) < n_slots:
            return None

        return slots


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

        ranks_per_node = td['ranks_per_node']
        cores_per_slot = td['cores_per_rank']
        gpus_per_slot  = td['gpus_per_rank']
        lfs_per_slot   = td['lfs_per_rank']
        mem_per_slot   = td['mem_per_rank']
        req_slots      = td['ranks']

        # make sure that processes are at least single-threaded
        if not cores_per_slot:
            cores_per_slot = 1

        self._log.debug_7('req : %s %s %s %s %s',
                          req_slots, cores_per_slot, gpus_per_slot,
                                     lfs_per_slot, mem_per_slot)

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

        if ranks_per_node:
            slots_per_node = min(slots_per_node, ranks_per_node)

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
        if self._partitions:
            if partition_id not in self._partitions:
                raise ValueError('partition id (%d) out of range'
                                 % partition_id)

            # partition id becomes a part of a co-locate tag
            colo_tag = str(partition_id) + ('' if not colo_tag else '_%s' % colo_tag)
            if colo_tag not in self._colo_history:
                self._colo_history[colo_tag] = self._partitions[partition_id]
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
            if self._partitions:
                # nodes assigned to the task should be from the same partition
                # FIXME: handle the case when unit (MPI task) would require
                #        more nodes than the amount available per partition
                _skip_node = True
                for plabel, p_node_indexs in self._partitions.items():
                    if node_index in p_node_indexs:
                        if task_partition_id in [None, plabel]:
                            node_partition_id = plabel
                            _skip_node = False
                        break
                if _skip_node:
                    continue

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

