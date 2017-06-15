
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from .base import AgentSchedulingComponent

import inspect
import threading as mt


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
# but allows to keep track of resource usage:
#
#    nodelist = [{name : 'node_1', cores: [................], gpus : [..]},
#                {name : 'node_2', cores: {................], gpus : [..]},
#                ...
#               ]
#
# When allocating a set of resource for a unit (2 cores, 1 gpu), we can now
# record those as used:
#
#    nodelist = [{name : 'node_1', cores: [##..............], gpus : [#.]},
#                {name : 'node_2', cores: {................], gpus : [..]},
#                ...
#               ]
#
# This solves the second part from our list above.  The third part, unit
# requirements, are obtained from the unit dict passed for scheduling.
#


# ------------------------------------------------------------------------------
#
# FIXME: make runtime switch depending on cprofile availability
import cProfile
cprof = cProfile.Profile()

def cprof_it(func):
    def wrapper(*args, **kwargs):
        retval = cprof.runcall(func, *args, **kwargs)
        return retval
    return wrapper

def dec_all_methods(dec):
    def dectheclass(cls):
        self_thread = mt.current_thread()
        if self_thread.name == 'MainThread' and \
                "CONTINUOUS" in os.getenv("RADICAL_PILOT_CPROFILE_COMPONENTS", "").split():
            for name, m in inspect.getmembers(cls, inspect.ismethod):
                setattr(cls, name, dec(m))
        return cls
    return dectheclass


# ------------------------------------------------------------------------------
#
@dec_all_methods(cprof_it)
class Continuous(AgentSchedulingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.slots = None

        AgentSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        if "CONTINUOUS" in os.getenv("RADICAL_PILOT_CPROFILE_COMPONENTS", "").split():
            self_thread = mt.current_thread()
            cprof.dump_stats("python-%s.profile" % self_thread.name)

        # make sure that parent finalizers are called
        super(Continuous, self).finalize_child()


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        if not self._lrms_node_list:
            raise RuntimeError("LRMS %s didn't _configure node_list." % \
                               self._lrms_info['name'])

        if not self._lrms_cores_per_node:
            raise RuntimeError("LRMS %s didn't _configure cores_per_node." % \
                               self._lrms_info['name'])

        if not self._lrms_gpus_per_node:
            raise RuntimeError("LRMS %s didn't _configure gpus_per_node." % \
                               self._lrms_info['name'])

        # Slots represents the internal process management structure.
        # The structure is as follows (cpn: cores per node, gpn: gpus per node
        # [
        #    {'node':'n_1', 'cores':[p_1,... ,p_cpn], 'gpus':[g_1,... ,g_gpn]},
        #    {'node':'n_2', 'cores':[p_1,... ,p_cpn], 'gpus':[g_1,... ,g_gpn]}
        # ]
        #
        # We put it in a list because we care about (and make use of) the order.
        #
        self.slots = []
        for node in self._lrms_node_list:
            self.slots.append({
                'node': node,
                # TODO: use real core numbers for non-exclusive reservations
                'cores': [rpc.FREE for _ in range(0, self._lrms_cores_per_node)],
                'gpus' : [rpc.FREE for _ in range(0, self._lrms_gpus_per_node)]
            })


    # --------------------------------------------------------------------------
    #
    def slot_status(self):
        """Returns a multi-line string corresponding to slot status.
        """

        slot_matrix = ""
        for slot in self.slots:
            slot_matrix += "|"
            for core in slot['cores']:
                if core == rpc.FREE:
                    slot_matrix += "-"
                else:
                    slot_matrix += "+"
        slot_matrix += "|"
        return {'timestamp' : time.time(),
                'slotstate' : slot_matrix}


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, cores_requested, gpus_requested):

        # TODO: single_node should be enforced for e.g. non-message passing
        #       tasks, but we don't have that info here.
        if  cores_requested <= self._lrms_cores_per_node and \
            gpus_requested  <= self._lrms_gpus_per_node:
            task_slots = self._find_slots_single_cont(cores_requested, gpus_requested)
        else:
            task_slots = self._find_slots_multi_cont(cores_requested, gpus_requested)

        if not task_slots:
            # allocation failed
            return {}

        self._change_slot_states(task_slots, rpc.BUSY)
        task_offsets = self.slots2offset(task_slots)

        return {'task_slots'   : task_slots,
                'task_offsets' : task_offsets,
                'lm_info'      : self._lrms_lm_info}


    # --------------------------------------------------------------------------
    #
    # Convert a set of slots into an index into the global slots list
    #
    def slots2offset(self, task_slots):
        # TODO: This assumes all hosts have the same number of cores

        first_slot = task_slots[0]
        # Get the host and the core part
        [first_slot_host, first_slot_core] = first_slot.split(':')
        # Find the entry in the the all_slots list based on the host
        slot_entry = (slot for slot in self.slots if slot["node"] == first_slot_host).next()
        # Transform it into an index in to the all_slots list
        all_slots_slot_index = self.slots.index(slot_entry)

        return all_slots_slot_index * self._lrms_cores_per_node + int(first_slot_core)


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, opaque_slots):

        if not 'task_slots' in opaque_slots:
            raise RuntimeError('insufficient information to release slots via %s: %s' \
                    % (self.uid, opaque_slots))

        self._change_slot_states(opaque_slots['task_slots'], rpc.FREE)


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
    def _find_cores_cont(self, slot_cores, cores_requested, gpus_requested, status):
        # FIXME: GPU
        return self._find_sublist(slot_cores, [status for _ in range(cores_requested)])


    # --------------------------------------------------------------------------
    #
    # Find an available continuous slot within node boundaries.
    #
    def _find_slots_single_cont(self, cores_requested, gpus_requested):

        # FIXME GPU

        for slot in self.slots:
            slot_node = slot['node']
            slot_cores = slot['cores']

            slot_cores_offset = self._find_cores_cont(slot_cores,
                    cores_requested, gpus_requested, rpc.FREE)

            if slot_cores_offset is not None:
              # self._log.info('Node %s satisfies %d cores / %d gpus at offset %d',
              #               slot_node, cores_requested, gpus_requested, slot_cores_offset)
                # FIXME GPU
                return ['%s:%d' % (slot_node, core) for core in
                        range(slot_cores_offset, slot_cores_offset + cores_requested)]

        return None


    # --------------------------------------------------------------------------
    #
    # Find an available continuous slot across node boundaries.
    #
    def _find_slots_multi_cont(self, cores_requested, gpu_requested):

        # Convenience aliases
        cores_per_node = self._lrms_cores_per_node
        all_slots = self.slots

        # Glue all slot core lists together
        all_slot_cores = [core for node in [node['cores'] for node in all_slots] for core in node]
        # self._log.debug("all_slot_cores: %s", all_slot_cores)

        # Find the start of the first available region
        all_slots_first_core_offset = self._find_cores_cont(all_slot_cores, 
                                      cores_requested, gpus_requested, rpc.FREE)
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
        last_slot = self.slots[last_slot_index]
        self._log.debug("last_slot: %s", last_slot)
        last_node = last_slot['node']
        self._log.debug("last_node: %s", last_node)
        first_slot = self.slots[first_slot_index]
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
    def _change_slot_states(self, task_slots, new_state):

        # Convenience alias
        all_slots = self.slots

        # self._log.debug("change_slot_states: unit slots: %s", task_slots)

        for slot in task_slots:
            # self._log.debug("change_slot_states: slot content: %s", slot)
            # Get the node and the core part
            [slot_node, slot_core] = slot.split(':')
            # Find the entry in the the all_slots list
            slot_entry = (slot for slot in all_slots if slot["node"] == slot_node).next()
            # Change the state of the slot
            slot_entry['cores'][int(slot_core)] = new_state


# ------------------------------------------------------------------------------

