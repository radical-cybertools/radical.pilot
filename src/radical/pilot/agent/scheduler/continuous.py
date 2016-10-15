
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import time

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from .base import AgentSchedulingComponent


# ==============================================================================
#
class Continuous(AgentSchedulingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        AgentSchedulingComponent.__init__(self, cfg)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        if not self._lrms_node_list:
            raise RuntimeError("LRMS %s didn't _configure node_list." % \
                               self._lrms_info['name'])

        if not self._lrms_cores_per_node:
            raise RuntimeError("LRMS %s didn't _configure cores_per_node." % \
                               self._lrms_info['name'])

        self._partitions = dict()
        free_cores = len(self._lrms_node_list) * self._lrms_cores_per_node

        # first determine the partition configuration, and determine how many
        # cores go into each partition.
        if  'scheduler'  in self._cfg and \
            'partitions' in self._cfg['agent_scheduler_cfg']:
            parts = self._cfg['agent_scheduler_cfg']['partitions']


        # first collect all partitions where an exact node count id given
        for p in parts:
            if isinstance(parts[p], int):
                free_cores         -= parts[p]
                self._partitions[p] = {'size'  : parts[p], 
                                       'nodes' : dict(),
                                       'slots' : list()}

        # then assign any leftover cores to a 'max' partition, if one is given.
        for p in parts:
            if isinstance(parts[p], basestring) and parts[p] == 'max':
                if not free_cores:
                    raise ValueError('no free cores left for max partition')
                self._partitions[p] = {'size'  : free_cores,
                                       'nodes' : dict(),
                                       'slots' : list()}
                free_cores = 0

        # we always add a 'default' partition for the remaining cores -- even if
        # it might empty
        self._partitions['_default'] = {'size'  : free_cores,
                                        'nodes' : dict(),
                                        'slots' : list()}

        # Slots represents the internal process management structure.
        # The structure is as follows:
        # [
        #    {'node': 'node1', 'cores': [p_1, p_2, p_3, ... , p_cores_per_node]},
        #    {'node': 'node2', 'cores': [p_1, p_2, p_3. ... , p_cores_per_node]
        # ]
        #
        # We create one such structure per partition.
        # We put it in a list because we care about (and make use of) the order.
        #
        cpn      = self._lrms_cores_per_node
        nodes    = self._lrms_node_list
        node_idx = 0
        core_idx = 0
        for pname, part in self._partitions.iteritems():
            psize  = part['size']
            pcores = 0
            while pcores < psize:

                assert(node_idx < len(nodes))
                node = nodes[node_idx]

                if node not in part['nodes']:
                    part['nodes'][node] = list()
                part['nodes'][node].append(rpc.FREE)

                pcores   += 1
                core_idx += 1

                if core_idx == cpn:
                    node_idx += 1
                    core_idx  = 0

        for pname, part in self._partitions.iteritems():
            for node in part['nodes']:
                self._partitions[pname]['slots'].append({
                    'node' : node,
                    'cores': part['nodes'][node]
                })
            del(part['nodes'])
                

        import pprint
        self._log.debug('partitions: %s', pprint.pformat(self._partitions))


    # --------------------------------------------------------------------------
    #
    def slot_status(self):
        """Returns a multi-line string corresponding to slot status.
        """

        slot_matrix = ""
        for pname,part in self._partitions.iteritems():
            for slot in part['slots']:
                slot_matrix += "|"
                for core in slot['cores']:
                    if core == rpc.FREE:
                        slot_matrix += "-"
                    else:
                        slot_matrix += "+"
            slot_matrix += "|"
        return {'timestamp' : rpu.timestamp(),
                'slotstate' : slot_matrix}


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, cu):

        cud = cu['description']

        cores = cud['cores']
        hints = cu['description'].get('scheduler_hint')
        if hints:
            pname = hints.get('partition', '_default')
        else:
            pname = '_default'

        # TODO: single_node should be enforced for e.g. non-message passing
        #       tasks, but we don't have that info here.
        if cores <= self._lrms_cores_per_node:
            single_node = True
        else:
            single_node = False

        # Given that we are the continuous scheduler, this is fixed.
        # TODO: Argument can be removed altogether?
        continuous = True

        # Switch between searching for continuous or scattered slots
        # Switch between searching for single or multi-node
        if single_node:
            if continuous:
                task_slots = self._find_slots_single_cont(cores, pname)
            else:
                raise NotImplementedError('No scattered single node scheduler implemented yet.')
        else:
            if continuous:
                task_slots = self._find_slots_multi_cont(cores, pname)
            else:
                raise NotImplementedError('No scattered multi node scheduler implemented yet.')

        if not task_slots:
            # allocation failed
            return {}

        self._change_slot_states(task_slots, pname, rpc.BUSY)
        task_offsets = self.slots2offset(task_slots, pname)

        return {'task_slots'   : task_slots,
                'task_offsets' : task_offsets,
                'partition'    : pname,
                'lm_info'      : self._lrms_lm_info}


    # --------------------------------------------------------------------------
    #
    # Convert a set of slots into an index into the global slots list
    #
    def slots2offset(self, task_slots, pname):

        # TODO: This assumes all hosts have the same number of cores
        first_slot = task_slots[0]
        slots      = self._partitions[pname]['slots']

        # Get the host and the core part
        [first_slot_host, first_slot_core] = first_slot.split(':')

        # Find the entry in the the all_slots list based on the host
        slot_entry = (slot for slot in slots if slot["node"] == first_slot_host).next()

        # Transform it into an index in to the all_slots list
        all_slots_slot_index = slots.index(slot_entry)

        return all_slots_slot_index * self._lrms_cores_per_node + int(first_slot_core)


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, opaque_slots):

        if not 'task_slots' in opaque_slots:
            raise RuntimeError('insufficient information to release slots via %s: %s' \
                    % (self.name, opaque_slots))

        self._change_slot_states(opaque_slots['task_slots'], 
                                 opaque_slots['partition'], rpc.FREE)


    # --------------------------------------------------------------------------
    #
    # Find a needle (continuous sub-list) in a haystack (list)
    #
    def _find_sublist(self, haystack, needle):
        n = len(needle)
        # Find all matches (returns list of False and True for every position)
        hits = [(needle == haystack[i:i+n]) for i in xrange(len(haystack)-n+1)]
        try:
            # Grab the first occurrence
            index = hits.index(True)
        except ValueError:
            index = None

        return index


    # --------------------------------------------------------------------------
    #
    # Transform the number of cores into a continuous list of "status"es,
    # and use that to find a sub-list.
    #
    def _find_cores_cont(self, slot_cores, cores_requested, status):
        return self._find_sublist(slot_cores, [status for _ in range(cores_requested)])


    # --------------------------------------------------------------------------
    #
    # Find an available continuous slot within node boundaries.
    #
    def _find_slots_single_cont(self, cores_requested, pname):

        slots = self._partitions[pname]['slots']

        for slot in slots:
            slot_node  = slot['node']
            slot_cores = slot['cores']

            slot_cores_offset = self._find_cores_cont(slot_cores,
                    cores_requested, rpc.FREE)

            if slot_cores_offset is not None:
              # self._log.info('Node %s satisfies %d cores at offset %d',
              #               slot_node, cores_requested, slot_cores_offset)
                return ['%s:%d' % (slot_node, core) for core in
                        range(slot_cores_offset, slot_cores_offset + cores_requested)]

        return None


    # --------------------------------------------------------------------------
    #
    # Find an available continuous slot across node boundaries.
    #
    def _find_slots_multi_cont(self, cores_requested, pname):

        slots = self._partitions[pname]['slots']

        # Convenience aliases
        cores_per_node = self._lrms_cores_per_node
        all_slots = slots

        # Glue all slot core lists together
        all_slot_cores = [core for node in [node['cores'] for node in all_slots] for core in node]
        # self._log.debug("all_slot_cores: %s", all_slot_cores)

        # Find the start of the first available region
        all_slots_first_core_offset = self._find_cores_cont(all_slot_cores, 
                cores_requested, rpc.FREE)
        self._log.debug("all_slots_first_core_offset: %s", all_slots_first_core_offset)
        if all_slots_first_core_offset is None:
            return None

        # Determine the first slot in the slot list
        first_slot_index = all_slots_first_core_offset / cores_per_node
        self._log.debug("first_slot_index: %s", first_slot_index)
        # And the core offset within that node
        first_slot_core_offset = all_slots_first_core_offset % cores_per_node
        self._log.debug("first_slot_core_offset: %s", first_slot_core_offset)

        # Note: We subtract one here, because counting starts at zero;
        #       Imagine a zero offset and a count of 1, the only core used
        #       would be core 0.
        #       TODO: Verify this claim :-)
        all_slots_last_core_offset = (first_slot_index * cores_per_node) +\
                                     first_slot_core_offset + cores_requested - 1
        self._log.debug("all_slots_last_core_offset: %s", all_slots_last_core_offset)
        last_slot_index = (all_slots_last_core_offset) / cores_per_node
        self._log.debug("last_slot_index: %s", last_slot_index)
        last_slot_core_offset = all_slots_last_core_offset % cores_per_node
        self._log.debug("last_slot_core_offset: %s", last_slot_core_offset)

        # Convenience aliases
        last_slot = slots[last_slot_index]
        self._log.debug("last_slot: %s", last_slot)
        last_node = last_slot['node']
        self._log.debug("last_node: %s", last_node)
        first_slot = slots[first_slot_index]
        self._log.debug("first_slot: %s", first_slot)
        first_node = first_slot['node']
        self._log.debug("first_node: %s", first_node)

        # Collect all node:core slots here
        task_slots = []

        # Add cores from first slot for this unit
        # As this is a multi-node search, we can safely assume that we go
        # from the offset all the way to the last core.
        task_slots.extend(['%s:%d' % (first_node, core) for core in
                           range(first_slot_core_offset, cores_per_node)])

        # Add all cores from "middle" slots
        for slot_index in range(first_slot_index+1, last_slot_index):
            slot_node = all_slots[slot_index]['node']
            task_slots.extend(['%s:%d' % (slot_node, core) for core in range(0, cores_per_node)])

        # Add the cores of the last slot
        task_slots.extend(['%s:%d' % (last_node, core) for core in range(0, last_slot_core_offset+1)])

        return task_slots


    # --------------------------------------------------------------------------
    #
    # Change the reserved state of slots (rpc.FREE or rpc.BUSY)
    #
    def _change_slot_states(self, task_slots, pname, new_state):

        slots = self._partitions[pname]['slots']

        # Convenience alias
        all_slots = slots

        for slot in task_slots:
            # Get the node and the core part
            [slot_node, slot_core] = slot.split(':')
            # Find the entry in the the all_slots list
            slot_entry = (slot for slot in all_slots if slot["node"] == slot_node).next()
            # Change the state of the slot
            slot_entry['cores'][int(slot_core)] = new_state


# ------------------------------------------------------------------------------

