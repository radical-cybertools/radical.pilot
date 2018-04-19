
__copyright__ = "Copyright 2017, http://radical.rutgers.edu"
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
class Hombre(AgentSchedulingComponent):
    '''
    HOMBRE: HOMogeneous Bag-of-task REsource allocator.  Don't kill me...

    The scheduler implementation manages a set of resources (cores).  When it
    gets the first scheduling request, it will assume that to be representative
    *for all future sccheduling requests*, ie. it assumes that all tasks are
    homogeneous.  It thus splits the resources into equal sized chunks and
    assigns them to the incoming requests.  The chunking is fast, no decisions
    need to be made, the scheduler really just hands out pre-defined chunks,
    which is relatively quick - that's the point, to trade generality with
    performance.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        super(Hombre, self).finalize_child()


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        if not self._lrms_node_list:
            raise RuntimeError("LRMS %s didn't _configure node_list." % \
                               self._lrms_info['name'])

        if not self._lrms_cores_per_node:
            raise RuntimeError("LRMS %s didn't _configure cores_per_node." % \
                               self._lrms_info['name'])

        self.chunk = None
        self.cpn   = self._lrms_cores_per_node
        self.free  = list()     # list of free chunks
        self.lock  = mt.Lock()  # lock for the above list
        self.slots = []         # list of available slots (one entry per core)
        self.index = 0          # scheduling position
        offset     = 0

        for node in self._lrms_node_list:
            for core in range(self.cpn):
                self.slots.append(['%s:%d' % (node, core), offset])
                offset += 1


    # --------------------------------------------------------------------------
    #
    def slot_status(self):

        return {'timestamp' : time.time(),
                'slotstate' : '???'}


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, cores_requested):

        if self.chunk is None:
            # this is the first request - use it to define the chunk size
            self.chunk = cores_requested

        else:
            # this is *not* the first request - make sure it has the same size
            if cores_requested != self.chunk:
                raise ValueError('hetbre?  %d != %d' % (self.chunk,
                                                        cores_requested))

        slots, offset = self._find_slots(cores_requested)

        if not slots:
            return None

        return {'task_slots'   : slots,
                'task_offsets' : offset,
                'lm_info'      : self._lrms_lm_info}


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, opaque_slots):

        with self.lock:
            self.free.append([opaque_slots['task_slots'],
                              opaque_slots['task_offsets']])


    # --------------------------------------------------------------------------
    #
    def _find_slots(self, cores_requested):

        # check if we have free chunks laying around - return one
        with self.lock:
            if self.free:
                return self.free.pop()

        # no used free chunk - generate a new chunk from the node list
        if (self.index + cores_requested) > len(self.slots):
            # out of resources
            return None, None

        slots  = list()
        offset = self.slots[self.index][1]  # offset of first core
        for i in range(cores_requested):
            slots.append(self.slots[self.index][0])
            self.index += 1

        return slots, offset


# ------------------------------------------------------------------------------

