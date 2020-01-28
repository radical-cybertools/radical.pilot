
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import time
import math

from ... import constants as rpc

from .base import AgentSchedulingComponent


# ------------------------------------------------------------------------------
#
class Torus(AgentSchedulingComponent):

    # TODO: Ultimately all BG/Q specifics should move out of the scheduler

    # --------------------------------------------------------------------------
    #
    # Offsets into block structure
    #
    TORUS_BLOCK_INDEX  = 0
    TORUS_BLOCK_COOR   = 1
    TORUS_BLOCK_NAME   = 2
    TORUS_BLOCK_STATUS = 3


    # --------------------------------------------------------------------------
    def __init__(self, cfg, session):

        self.nodes            = None
        self._cores_per_node  = None

        AgentSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        if not self._rm_cores_per_node:
            raise RuntimeError("ResourceManager %s didn't _configure cores_per_node." %
                               self._rm_info['name'])

        self._cores_per_node = self._rm_cores_per_node

        # TODO: get rid of field below
        self.nodes = 'bogus'


    # --------------------------------------------------------------------------
    #
    def slot_status(self):
        """Returns a multi-line string corresponding to slot status.
        """

        slot_matrix = ""
        for slot in self._rm.torus_block:
            slot_matrix += "|"
            if slot[self.TORUS_BLOCK_STATUS] == rpc.FREE:
                slot_matrix += "-" * self._rm_cores_per_node
            else:
                slot_matrix += "+" * self._rm_cores_per_node
        slot_matrix += "|"
        return {'timestamp': time.time(),
                'slotstate': slot_matrix}


    # --------------------------------------------------------------------------
    #
    # Allocate a number of cores
    #
    # Currently only implements full-node allocation, so core count must
    # be a multiple of cores_per_node.
    #
    def _allocate_slot(self, cores_requested, gpus_requested):

        block = self._rm.torus_block
        sub_block_shape_table = self._rm.shape_table

        self._log.info("Trying to allocate %d core(s / %d gpus.",
                cores_requested, gpus_requested)

        # FIXME GPU

        if cores_requested % self._rm_cores_per_node:
            num_cores = int(math.ceil(cores_requested / float(self._rm_cores_per_node))) \
                        * self._rm_cores_per_node
            self._log.error('Core not multiple of %d, increasing to %d!',
                           self._rm_cores_per_node, num_cores)

        num_nodes = cores_requested / self._rm_cores_per_node

        offset = self._alloc_sub_block(block, num_nodes)

        if offset is None:
            self._log.warning('No allocation made.')
            return

        # TODO: return something else than corner location? Corner index?
        sub_block_shape     = sub_block_shape_table[num_nodes]
        sub_block_shape_str = self._rm.shape2str(sub_block_shape)
        corner              = block[offset][self.TORUS_BLOCK_COOR]
        corner_offset       = self.corner2offset(self._rm.torus_block, corner)
        corner_node         = self._rm.torus_block[corner_offset][self.TORUS_BLOCK_NAME]

        end = self.get_last_node(corner, sub_block_shape)
        self._log.debug('Allocating sub-block of %d node(s) with dimensions %s'
                       ' at offset %d with corner %s and end %s.',
                        num_nodes, sub_block_shape_str, offset,
                        self._rm.loc2str(corner), self._rm.loc2str(end))

        return {'cores_per_node'      : self._rm_cores_per_node,
                'loadl_bg_block'      : self._rm.loadl_bg_block,
                'sub_block_shape_str' : sub_block_shape_str,
                'corner_node'         : corner_node,
                'lm_info'             : self._rm_lm_info}


    # --------------------------------------------------------------------------
    #
    # Allocate a sub-block within a block
    # Currently only works with offset that are exactly the sub-block size
    #
    def _alloc_sub_block(self, block, num_nodes):

        offset = 0
        # Iterate through all nodes with offset a multiple of the sub-block size
        while True:

            # Verify the assumption (needs to be an assert?)
            if offset % num_nodes != 0:
                msg = 'Sub-block needs to start at correct offset!'
                self._log.exception(msg)
                raise ValueError(msg)
                # TODO: If we want to workaround this, the coordinates need to overflow

            not_free = False
            # Check if all nodes from offset till offset+size are rpc.FREE
            for peek in range(num_nodes):
                try:
                    if block[offset + peek][self.TORUS_BLOCK_STATUS] \
                                                                    == rpc.BUSY:
                        # Once we find the first rpc.BUSY node we
                        # can discard this attempt
                        not_free = True
                        break
                except IndexError:
                    self._log.exception('Block out of bound. Num_nodes: %d,'
                                        'offset: %d, peek: %d.',
                                        num_nodes, offset, peek)

            if not_free is True:
                # No success at this offset
                self._log.info("No free nodes found at this offset: %d.", offset)

                # If we weren't the last attempt, then increase the offset and iterate again.
                if offset + num_nodes < self._block2num_nodes(block):
                    offset += num_nodes
                    continue
                else:
                    return

            else:
                # At this stage we have found a free spot!

                self._log.info("Free nodes found at this offset: %d.", offset)

                # Then mark the nodes busy
                for peek in range(num_nodes):
                    block[offset + peek][self.TORUS_BLOCK_STATUS] = rpc.BUSY

                return offset


    # --------------------------------------------------------------------------
    #
    # Return the number of nodes in a block
    #
    def _block2num_nodes(self, block):
        return len(block)


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, xxx_todo_changeme):
        (corner, shape) = xxx_todo_changeme
        self._free_cores(self._rm.torus_block, corner, shape)


    # --------------------------------------------------------------------------
    #
    # Free up an allocation
    #
    def _free_cores(self, block, corner, shape):

        # Number of nodes to free
        num_nodes = self._shape2num_nodes(shape)

        # Location of where to start freeing
        offset = self.corner2offset(block, corner)

        self._log.info("Freeing %d nodes starting at %d.", num_nodes, offset)

        for peek in range(num_nodes):
            assert block[offset + peek][self.TORUS_BLOCK_STATUS] == rpc.BUSY, \
                   'Block %d not Free!' % block[offset + peek]
            block[offset + peek][self.TORUS_BLOCK_STATUS] = rpc.FREE


    # --------------------------------------------------------------------------
    #
    # Follow coordinates to get the last node
    #
    def get_last_node(self, origin, shape):

        ret = dict()

        for dim in self._rm.torus_dimension_labels:
            ret[dim] = origin[dim] + shape[dim] - 1
        return ret


    # --------------------------------------------------------------------------
    #
    # Return the number of nodes for the given block shape
    #
    def _shape2num_nodes(self, shape):

        nodes = 1
        for dim in self._rm.torus_dimension_labels:
            nodes *= shape[dim]

        return nodes


    # --------------------------------------------------------------------------
    #
    # Return the offset into the node list from a corner
    #
    # TODO: Can this be determined instead of searched?
    #
    def corner2offset(self, block, corner):
        offset = 0

        for e in block:
            if corner == e[self.TORUS_BLOCK_COOR]:
                return offset
            offset += 1

        return offset


# ------------------------------------------------------------------------------

