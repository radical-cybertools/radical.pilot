
__copyright__ = "Copyright 2017, http://radical.rutgers.edu"
__license__   = "MIT"


import time
from ... import constants as rpc
from .base import AgentSchedulingComponent


# ==============================================================================
#
# Scattered core agent scheduler.
# Finds available cores within the job's allocation.
#
# TODO: For non-MPI multicore tasks, this provides no guarantee that they are
#       on the same host.
#
class Scattered(AgentSchedulingComponent):
    

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.slots = None
        AgentSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        if not self._lrms_node_list:
            raise RuntimeError("LRMS %s didn't _configure node_list." % \
                               self._lrms_info['name'])

        if not self._lrms_cores_per_node:
            raise RuntimeError("LRMS %s didn't _configure cores_per_node." % \
                               self._lrms_info['name'])

        # Slots represents the internal process management structure.
        # The structure is as follows:
        # [
        #    {'node': 'node1:0', 'state': rpc.FREE | rpc.BUSY},
        #    {'node': 'node2:0', 'state': rpc.FREE | rpc.BUSY},
        #     ....
        #    {'node': 'nodeN:0', 'state': rpc.FREE | rpc.BUSY}
        # ]
        #
        self.slots = []
        for node in self._lrms_node_list:
            self.slots.append({
                'node': '%s:0' % node,
                'state': rpc.FREE
            })


    # --------------------------------------------------------------------------
    #
    # Provide visual presentation of slot status
    #
    def slot_status(self):
        """Returns a multi-line string corresponding to slot status.
        """

        slot_matrix = ""
        for slot in self.slots:
            slot_matrix += "|"
            if slot['state'] == rpc.FREE:
                slot_matrix += "-"
            else:
                slot_matrix += "+"
        slot_matrix += "|"
        return {'timestamp' : time.time(),
                'slotstate' : slot_matrix}


    # --------------------------------------------------------------------------
    #
    # Find cores and allocate them if available
    #
    def _allocate_slot(self, cores_requested):

        offsets = self._find_slots_scattered(cores_requested)

        if not offsets:
            # allocation failed
            return {}

        task_slots = [self.slots[x]['node'] for x in offsets]

        self._change_slot_states(offsets, rpc.BUSY)

        result = {
            'task_slots'   : task_slots,
            'task_offsets' : offsets,
            'lm_info'      : self._lrms_lm_info
        }

        self._log.debug('Slot allocated: %s', result)

        return result


    # --------------------------------------------------------------------------
    #
    # Release cores associated to this slot
    #
    def _release_slot(self, opaque_slots):

        if not 'task_offsets' in opaque_slots:
            raise RuntimeError('insufficient information to release slots via %s: %s' \
                    % (self.name, opaque_slots))

        self._change_slot_states(opaque_slots['task_offsets'], rpc.FREE)


    # --------------------------------------------------------------------------
    #
    # Find available cores
    #
    def _find_slots_scattered(self, cores_requested):

        offsets = []
        for offset, slot in enumerate(self.slots):

            if slot['state'] == rpc.FREE:
                offsets.append(offset)

            if len(offsets) == cores_requested:
                return offsets

        return None


    # --------------------------------------------------------------------------
    #
    # Change the reserved state of slots at the offsets(rpc.FREE or rpc.BUSY)
    #
    def _change_slot_states(self, offsets, new_state):

        for offset in offsets:
            self.slots[offset]['state'] = new_state

# ------------------------------------------------------------------------------

