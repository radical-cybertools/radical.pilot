
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

        self.nodes = None
        AgentSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # TODO: use real core/gpu numbers for non-exclusive reservations

        self.nodes = []
        for node in self._lrms_node_list:
            self.nodes.append({
                'name' : '%s:0' % node,
                'cores': rpc.FREE * self._lrms_cores_per_node,
                'gpus' : rpc.FREE * self._lrms_gpus_per_node
            })


    # --------------------------------------------------------------------------
    #
    # Provide visual presentation of slot status
    #
    def slot_status(self):
        """Returns a multi-line string corresponding to slot status.
        """

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
    # Find cores and allocate them if available
    #
    def _allocate_slot(self, cud):

        cores_requested = cud['cpu_processes'] * cud['cpu_threads']
        gpus_requested  = cud['gpus']

        offsets = []
        for offset, node in enumerate(self.nodes):

            if node['state'] == rpc.FREE:
                offsets.append(offset)

            elif len(offsets) == cores_requested:
                break

        if len(offsets) < cores_requested:
            return {}   # allocation failed

        task_slots = [self.nodes[x]['name'] for x in offsets]

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
    def _release_slot(self, slots):

        if not 'task_offsets' in slots:
            raise RuntimeError('insufficient information to release slots via %s: %s' \
                    % (self.name, slots))

        self._change_slot_states(slots['task_offsets'], rpc.FREE)


    # --------------------------------------------------------------------------
    #
    # Change the reserved state of slots at the offsets(rpc.FREE or rpc.BUSY)
    #
    def _change_slot_states(self, offsets, new_state):

        for offset in offsets:
            self.nodes[offset]['state'] = new_state

# ------------------------------------------------------------------------------

