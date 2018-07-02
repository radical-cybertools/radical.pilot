
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time
import subprocess

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from ..scheduler.torus import Torus

from base import LRMS


# ==============================================================================
#
class LoadLeveler(LRMS):

    # --------------------------------------------------------------------------
    #
    # BG/Q Topology of Nodes within a Board
    #
    BGQ_BOARD_TOPO = {
        0: {'A': 29, 'B':  3, 'C':  1, 'D': 12, 'E':  7},
        1: {'A': 28, 'B':  2, 'C':  0, 'D': 13, 'E':  6},
        2: {'A': 31, 'B':  1, 'C':  3, 'D': 14, 'E':  5},
        3: {'A': 30, 'B':  0, 'C':  2, 'D': 15, 'E':  4},
        4: {'A': 25, 'B':  7, 'C':  5, 'D':  8, 'E':  3},
        5: {'A': 24, 'B':  6, 'C':  4, 'D':  9, 'E':  2},
        6: {'A': 27, 'B':  5, 'C':  7, 'D': 10, 'E':  1},
        7: {'A': 26, 'B':  4, 'C':  6, 'D': 11, 'E':  0},
        8: {'A': 21, 'B': 11, 'C':  9, 'D':  4, 'E': 15},
        9: {'A': 20, 'B': 10, 'C':  8, 'D':  5, 'E': 14},
        10: {'A': 23, 'B':  9, 'C': 11, 'D':  6, 'E': 13},
        11: {'A': 22, 'B':  8, 'C': 10, 'D':  7, 'E': 12},
        12: {'A': 17, 'B': 15, 'C': 13, 'D':  0, 'E': 11},
        13: {'A': 16, 'B': 14, 'C': 12, 'D':  1, 'E': 10},
        14: {'A': 19, 'B': 13, 'C': 15, 'D':  2, 'E':  9},
        15: {'A': 18, 'B': 12, 'C': 14, 'D':  3, 'E':  8},
        16: {'A': 13, 'B': 19, 'C': 17, 'D': 28, 'E': 23},
        17: {'A': 12, 'B': 18, 'C': 16, 'D': 29, 'E': 22},
        18: {'A': 15, 'B': 17, 'C': 19, 'D': 30, 'E': 21},
        19: {'A': 14, 'B': 16, 'C': 18, 'D': 31, 'E': 20},
        20: {'A':  9, 'B': 23, 'C': 21, 'D': 24, 'E': 19},
        21: {'A':  8, 'B': 22, 'C': 20, 'D': 25, 'E': 18},
        22: {'A': 11, 'B': 21, 'C': 23, 'D': 26, 'E': 17},
        23: {'A': 10, 'B': 20, 'C': 22, 'D': 27, 'E': 16},
        24: {'A':  5, 'B': 27, 'C': 25, 'D': 20, 'E': 31},
        25: {'A':  4, 'B': 26, 'C': 24, 'D': 21, 'E': 30},
        26: {'A':  7, 'B': 25, 'C': 27, 'D': 22, 'E': 29},
        27: {'A':  6, 'B': 24, 'C': 26, 'D': 23, 'E': 28},
        28: {'A':  1, 'B': 31, 'C': 29, 'D': 16, 'E': 27},
        29: {'A':  0, 'B': 30, 'C': 28, 'D': 17, 'E': 26},
        30: {'A':  3, 'B': 29, 'C': 31, 'D': 18, 'E': 25},
        31: {'A':  2, 'B': 28, 'C': 30, 'D': 19, 'E': 24},
        }

    # --------------------------------------------------------------------------
    #
    # BG/Q Config
    #
    # FIXME: not used?
    #
    BGQ_CORES_PER_NODE      = 16
    BGQ_GPUS_PER_NODE       =  0 # FIXME GPU
    BGQ_NODES_PER_BOARD     = 32 # NODE       == Compute Card == Chip module
    BGQ_BOARDS_PER_MIDPLANE = 16 # NODE BOARD == NODE CARD
    BGQ_MIDPLANES_PER_RACK  =  2


    # --------------------------------------------------------------------------
    #
    # Default mapping = "ABCDE(T)"
    #
    # http://www.redbooks.ibm.com/redbooks/SG247948/wwhelp/wwhimpl/js/html/wwhelp.htm
    #
    BGQ_MAPPING = "ABCDE"


    # --------------------------------------------------------------------------
    #
    # Board labels (Rack, Midplane, Node)
    #
    BGQ_BOARD_LABELS = ['R', 'M', 'N']


    # --------------------------------------------------------------------------
    #
    # Dimensions of a (sub-)block
    #
    BGQ_DIMENSION_LABELS = ['A', 'B', 'C', 'D', 'E']


    # --------------------------------------------------------------------------
    #
    # Supported sub-block sizes (number of nodes).
    # This influences the effectiveness of mixed-size allocations
    # (and might even be a hard requirement from a topology standpoint).
    #
    # TODO: Do we actually need to restrict our sub-block sizes to this set?
    #
    BGQ_SUPPORTED_SUB_BLOCK_SIZES = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]


    # --------------------------------------------------------------------------
    #
    # Mapping of starting corners.
    #
    # "board" -> "node"
    #
    # Ordering: ['E', 'D', 'DE', etc.]
    #
    # TODO: Is this independent of the mapping?
    #
    BGQ_BLOCK_STARTING_CORNERS = {
        0:  0,
        4: 29,
        8:  4,
        12: 25
    }


    # --------------------------------------------------------------------------
    #
    # BG/Q Topology of Boards within a Midplane
    #
    BGQ_MIDPLANE_TOPO = {
        0: {'A':  4, 'B':  8, 'C':  1, 'D':  2},
        1: {'A':  5, 'B':  9, 'C':  0, 'D':  3},
        2: {'A':  6, 'B': 10, 'C':  3, 'D':  0},
        3: {'A':  7, 'B': 11, 'C':  2, 'D':  1},
        4: {'A':  0, 'B': 12, 'C':  5, 'D':  6},
        5: {'A':  1, 'B': 13, 'C':  4, 'D':  7},
        6: {'A':  2, 'B': 14, 'C':  7, 'D':  4},
        7: {'A':  3, 'B': 15, 'C':  6, 'D':  5},
        8: {'A': 12, 'B':  0, 'C':  9, 'D': 10},
        9: {'A': 13, 'B':  1, 'C':  8, 'D': 11},
        10: {'A': 14, 'B':  2, 'C': 11, 'D':  8},
        11: {'A': 15, 'B':  3, 'C': 10, 'D':  9},
        12: {'A':  8, 'B':  4, 'C': 13, 'D': 14},
        13: {'A':  9, 'B':  5, 'C': 12, 'D': 15},
        14: {'A': 10, 'B':  6, 'C': 15, 'D': 12},
        15: {'A': 11, 'B':  7, 'C': 14, 'D': 13},
        }

    # --------------------------------------------------------------------------
    #
    # Shape of whole BG/Q Midplane
    #
    BGQ_MIDPLANE_SHAPE = {'A': 4, 'B': 4, 'C': 4, 'D': 4, 'E': 2} # '4x4x4x4x2'


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.torus_block            = None
        self.loadl_bg_block         = None
        self.shape_table            = None
        self.torus_dimension_labels = None

        LRMS.__init__(self, cfg, session)

    # --------------------------------------------------------------------------
    #
    def _configure(self):

        loadl_node_list     = None
        loadl_cpus_per_node = None

        # Determine method for determining hosts,
        # either through hostfile or BG/Q environment.
        loadl_hostfile      = os.environ.get('LOADL_HOSTFILE')
        self.loadl_bg_block = os.environ.get('LOADL_BG_BLOCK')

        if not loadl_hostfile and not self.loadl_bg_block:
            msg = "Neither $LOADL_HOSTFILE or $LOADL_BG_BLOCK set!"
            self._log.error(msg)
            raise RuntimeError(msg)

        # Determine the size of the pilot allocation
        if loadl_hostfile:
            # Non Blue Gene Load Leveler installation.

            loadl_total_tasks_str = os.environ.get('LOADL_TOTAL_TASKS')
            if loadl_total_tasks_str is None:
                msg = "$LOADL_TOTAL_TASKS not set!"
                self._log.error(msg)
                raise RuntimeError(msg)
            else:
                loadl_total_tasks = int(loadl_total_tasks_str)

            # Construct the host list
            loadl_nodes = [line.strip() for line in open(loadl_hostfile)]
            self._log.info("Found LOADL_HOSTFILE %s. Expanded to: %s",
                          loadl_hostfile, loadl_nodes)
            loadl_node_list = list(set(loadl_nodes))

            # Verify that $LLOAD_TOTAL_TASKS == len($LOADL_HOSTFILE)
            if loadl_total_tasks != len(loadl_nodes):
                self._log.error("$LLOAD_TOTAL_TASKS(%d) != len($LOADL_HOSTFILE)(%d)",
                               loadl_total_tasks, len(loadl_nodes))

            # Determine the number of cpus per node.  Assume:
            # cores_per_node = lenght(nodefile) / len(unique_nodes_in_nodefile)
            loadl_cpus_per_node = len(loadl_nodes) / len(loadl_node_list)
            loadl_gpus_per_node = self._cfg.get('gpus_per_node', 0) # FIXME GPU

        elif self.loadl_bg_block:
            # Blue Gene specific.
            loadl_bg_midplane_list_str = None
            loadl_bg_block_size_str    = None
            loadl_bg_board_list_str    = None
            loadl_bg_block_shape_str   = None
            loadl_job_name             = os.environ.get('LOADL_JOB_NAME')

            if not loadl_job_name:
                raise RuntimeError("$LOADL_JOB_NAME not set!")

            # Get the board list and block shape from 'llq -l' output
            output = subprocess.check_output(["llq", "-l", loadl_job_name])

            for line in output.splitlines():
                if "BG Node Board List: " in line:
                    loadl_bg_board_list_str    = line.split(':')[1].strip()
                elif "BG Midplane List: " in line:
                    loadl_bg_midplane_list_str = line.split(':')[1].strip()
                elif "BG Shape Allocated: " in line:
                    loadl_bg_block_shape_str   = line.split(':')[1].strip()
                elif "BG Size Allocated: " in line:
                    loadl_bg_block_size_str    = line.split(':')[1].strip()

            if not loadl_bg_board_list_str:
                raise RuntimeError("No board list found in llq output!")
            if not loadl_bg_midplane_list_str:
                raise RuntimeError("No midplane list found in llq output!")
            if not loadl_bg_block_shape_str:
                raise RuntimeError("No board shape found in llq output!")
            if not loadl_bg_block_size_str:
                raise RuntimeError("No board size found in llq output!")

            loadl_bg_block_size = int(loadl_bg_block_size_str)

            self._log.debug("BG Node Board List: %s" % loadl_bg_board_list_str)
            self._log.debug("BG Midplane List  : %s" % loadl_bg_midplane_list_str)
            self._log.debug("BG Shape Allocated: %s" % loadl_bg_block_shape_str)
            self._log.debug("BG Size Allocated : %d" % loadl_bg_block_size)

            # Build nodes data structure to be handled by Torus Scheduler
            try:
                self.torus_block = self._bgq_construct_block(
                    loadl_bg_block_shape_str, loadl_bg_board_list_str,
                    loadl_bg_block_size,      loadl_bg_midplane_list_str)
            except Exception as e:
                raise RuntimeError("Couldn't construct block: %s" % e.message)

            self._log.debug("Torus block constructed:")
            for e in self.torus_block:
                self._log.debug("%s %s %s %s" %
                     (e[0], [e[1][key] for key in sorted(e[1])], e[2], e[3]))

            try:
                loadl_node_list = [entry[Torus.TORUS_BLOCK_NAME] \
                                   for entry in self.torus_block]
            except Exception as e:
                raise RuntimeError("Couldn't construct node list")

            # Construct sub-block table
            try:
                self.shape_table = self._bgq_create_sub_block_shape_table(loadl_bg_block_shape_str)
            except Exception as e:
                raise RuntimeError("Couldn't construct shape table")

            self._log.debug("Node list constructed: %s" % loadl_node_list)
            self._log.debug("Shape table constructed: ")
            for (size, dim) in [(key, self.shape_table[key]) for key in sorted(self.shape_table)]:
                self._log.debug("%s %s", (size, [dim[key] for key in sorted(dim)]))

            # Determine the number of cpus per node
            loadl_cpus_per_node = self.BGQ_CORES_PER_NODE

            # BGQ Specific Torus labels
            self.torus_dimension_labels = self.BGQ_DIMENSION_LABELS

        # node names are unique, so can serve as node uids
        self.node_list      = [[node, node] for node in loadl_node_list]
        self.cores_per_node = loadl_cpus_per_node
        self.gpus_per_node  = loadl_gpus_per_node

        self._log.debug("Sleeping for #473 ...")
        time.sleep(5)
        self._log.debug("Configure done")


    # --------------------------------------------------------------------------
    #
    # Walk the block and return the node name for the given location
    #
    def _bgq_nodename_by_loc(self, midplanes, board, location):

        self._log.debug("Starting nodebyname - midplanes:%s, board:%d", midplanes, board)

        node = self.BGQ_BLOCK_STARTING_CORNERS[board]

        # TODO: Does the order of walking matter?
        #       It might because of the starting blocks ...
        for dim in self.BGQ_DIMENSION_LABELS: # [::-1]:
            max_length = location[dim]
            self._log.debug("Within dim loop dim:%s, max_length: %d", dim, max_length)

            cur_length = 0
            # Loop while we are not at the final depth
            while cur_length < max_length:
                self._log.debug("beginning of while loop, cur_length: %d", cur_length)

                if cur_length % 2 == 0:
                    # Stay within the board
                    node = self.BGQ_BOARD_TOPO[node][dim]

                else:
                    # We jump to another board.
                    self._log.debug("jumping to new board from board: %d, dim: %s)", board, dim)
                    board = self.BGQ_MIDPLANE_TOPO[board][dim]
                    self._log.debug("board is now: %d", board)

                    # If we switch boards in the B dimension,
                    # we seem to "land" at the opposite E dimension.
                    if dim  == 'B':
                        node = self.BGQ_BOARD_TOPO[node]['E']

                self._log.debug("node is now: %d", node)

                # Increase the length for the next iteration
                cur_length += 1

            self._log.debug("Wrapping inside dim loop dim:%s", dim)

        # TODO: This will work for midplane expansion in one dimension only
        midplane_idx = max(location.values()) / 4
        rack = midplanes[midplane_idx]['R']
        midplane = midplanes[midplane_idx]['M']

        nodename = 'R%.2d-M%.1d-N%.2d-J%.2d' % (rack, midplane, board, node)
        self._log.debug("from location %s constructed node name: %s, left at board: %d",
                        self.loc2str(location), nodename, board)

        return nodename


    # --------------------------------------------------------------------------
    #
    # Convert the board string as given by llq into a board structure
    #
    # E.g. 'R00-M1-N08,R00-M1-N09,R00-M1-N10,R00-M0-N11' =>
    # [{'R': 0, 'M': 1, 'N': 8}, {'R': 0, 'M': 1, 'N': 9},
    #  {'R': 0, 'M': 1, 'N': 10}, {'R': 0, 'M': 0, 'N': 11}]
    #
    def _bgq_str2boards(self, boards_str):

        boards = boards_str.split(',')

        board_dict_list = []

        for board in boards:
            elements = board.split('-')

            board_dict = {}
            for l, e in zip(self.BGQ_BOARD_LABELS, elements):
                board_dict[l] = int(e.split(l)[1])

            board_dict_list.append(board_dict)

        return board_dict_list


    # --------------------------------------------------------------------------
    #
    # Convert the midplane string as given by llq into a midplane structure
    #
    # E.g. 'R04-M0,R04-M1' =>
    # [{'R': 4, 'M': 0}, {'R': 4, 'M': 1}]
    #
    #
    def _bgq_str2midplanes(self, midplane_str):

        midplanes = midplane_str.split(',')

        midplane_dict_list = []
        for midplane in midplanes:
            elements = midplane.split('-')

            midplane_dict = {}
            # Take the first two labels
            for l, e in zip(self.BGQ_BOARD_LABELS[:2], elements):
                midplane_dict[l] = int(e.split(l)[1])

            midplane_dict_list.append(midplane_dict)

        return midplane_dict_list


    # --------------------------------------------------------------------------
    #
    # Convert the string as given by llq into a block shape structure:
    #
    # E.g. '1x2x3x4x5' => {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5}
    #
    def _bgq_str2shape(self, shape_str):

        # Get the lengths of the shape
        shape_lengths = shape_str.split('x', 4)

        shape_dict = {}
        for dim, length in zip(self.BGQ_DIMENSION_LABELS, shape_lengths):
            shape_dict[dim] = int(length)

        return shape_dict


    # --------------------------------------------------------------------------
    #
    # Multiply two shapes
    #
    def _multiply_shapes(self, shape1, shape2):

        result = {}

        for dim in self.BGQ_DIMENSION_LABELS:
            try:
                val1 = shape1[dim]
            except KeyError:
                val1 = 1

            try:
                val2 = shape2[dim]
            except KeyError:
                val2 = 1

            result[dim] = val1 * val2

        return result


    # --------------------------------------------------------------------------
    #
    # Convert location dict into a tuple string
    # E.g. {'A': 1, 'C': 4, 'B': 1, 'E': 2, 'D': 4} => '(1,4,1,2,4)'
    #
    def loc2str(self, loc):
        return str(tuple(loc[dim] for dim in self.BGQ_DIMENSION_LABELS))


    # --------------------------------------------------------------------------
    #
    # Convert a shape dict into string format
    #
    # E.g. {'A': 1, 'C': 4, 'B': 1, 'E': 2, 'D': 4} => '1x4x1x2x4'
    #
    def shape2str(self, shape):

        shape_str = ''
        for l in self.BGQ_DIMENSION_LABELS:

            # Get the corresponding count
            shape_str += str(shape[l])

            # Add an 'x' behind all but the last label
            if l in self.BGQ_DIMENSION_LABELS[:-1]:
                shape_str += 'x'

        return shape_str


    # --------------------------------------------------------------------------
    #
    # Return list of nodes that make up the block
    #
    # Format: [(index, location, nodename, status), (i, c, n, s), ...]
    #
    # TODO: This function and _bgq_nodename_by_loc should be changed so that we
    #       only walk the torus once?
    #
    def _bgq_get_block(self, midplanes, board, shape):

        self._log.debug("Shape: %s", shape)

        nodes = []
        index = 0

        for a in range(shape['A']):
            for b in range(shape['B']):
                for c in range(shape['C']):
                    for d in range(shape['D']):
                        for e in range(shape['E']):
                            location = {'A': a, 'B': b, 'C': c, 'D': d, 'E': e}
                            nodename = self._bgq_nodename_by_loc(midplanes, board, location)
                            nodes.append([index, location, nodename, rpc.FREE])
                            index += 1

        return nodes


    # --------------------------------------------------------------------------
    #
    # Use block shape and board list to construct block structure
    #
    # The 5 dimensions are denoted by the letters A, B, C, D, and E, T for the core (0-15).
    # The latest dimension E is always 2, and is contained entirely within a midplane.
    # For any compute block, compute nodes (as well midplanes for large blocks) are combined in 4 dimensions,
    # only 4 dimensions need to be considered.
    #
    #  128 nodes: BG Shape Allocated: 2x2x4x4x2
    #  256 nodes: BG Shape Allocated: 4x2x4x4x2
    #  512 nodes: BG Shape Allocated: 1x1x1x1
    #  1024 nodes: BG Shape Allocated: 1x1x1x2
    #
    def _bgq_construct_block(self, block_shape_str, boards_str,
                            block_size, midplane_list_str):

        llq_shape = self._bgq_str2shape(block_shape_str)

        # TODO: Could check this, but currently _shape2num is part of the other class
        #if self._shape2num_nodes(llq_shape) != block_size:
        #    self._log.error("Block Size doesn't match Block Shape")

        # If the block is equal to or greater than a Midplane,
        # then there is no board list provided.
        # But because at that size, we have only full midplanes,
        # we can construct it.

        if block_size >= 1024:
            #raise NotImplementedError("Currently multiple midplanes are not yet supported.")

            # BG Size: 1024, BG Shape: 1x1x1x2, BG Midplane List: R04-M0,R04-M1
            midplanes = self._bgq_str2midplanes(midplane_list_str)

            # Start of at the "lowest" available rack/midplane/board
            # TODO: No other explanation than that this seems to be the convention?
            # TODO: Can we safely assume that they are sorted?
            #rack = midplane_dict_list[0]['R']
            #midplane = midplane_dict_list[0]['M']
            board = 0

            # block_shape = llq_shape * BGQ_MIDPLANE_SHAPE
            block_shape = self._multiply_shapes(self.BGQ_MIDPLANE_SHAPE, llq_shape)
            self._log.debug("Resulting shape after multiply: %s", block_shape)

        elif block_size == 512:
            # Full midplane

            # BG Size: 1024, BG Shape: 1x1x1x2, BG Midplane List: R04-M0,R04-M1
            midplanes = self._bgq_str2midplanes(midplane_list_str)

            # Start of at the "lowest" available rack/midplane/board
            # TODO: No other explanation than that this seems to be the convention?
            #rack = midplane_dict_list[0]['R'] # Assume they are all equal
            #midplane = min([entry['M'] for entry in midplane_dict_list])
            board = 0

            block_shape = self.BGQ_MIDPLANE_SHAPE

        else:
            # Within single midplane, < 512 nodes

            board_dict_list = self._bgq_str2boards(boards_str)
            self._log.debug("Board dict list:\n%s", '\n'.join([str(x) for x in board_dict_list]))

            midplanes = [{'R': board_dict_list[0]['R'],
                          'M': board_dict_list[0]['M']}]

            # Start of at the "lowest" available board.
            # TODO: No other explanation than that this seems to be the convention?
            board = min([entry['N'] for entry in board_dict_list])

            block_shape = llq_shape

        # From here its all equal (assuming our walker does the walk and not just the talk!)
        block = self._bgq_get_block(midplanes, board, block_shape)

        # TODO: Check returned block:
        #       - Length
        #       - No duplicates

        return block


    # --------------------------------------------------------------------------
    #
    # Construction of sub-block shapes based on overall block allocation.
    #
    # Depending on the size of the total allocated block, the maximum size
    # of a subblock can be 512 nodes.
    #
    #
    def _bgq_create_sub_block_shape_table(self, shape_str):

        # Convert the shape string into dict structure
        #
        # For < 512 nodes: the dimensions within a midplane (AxBxCxDxE)
        # For >= 512 nodes: the dimensions between the midplanes (AxBxCxD)
        #
        if len(shape_str.split('x')) == 5:
            block_shape = self._bgq_str2shape(shape_str)
        elif len(shape_str.split('x')) == 4:
            block_shape = self.BGQ_MIDPLANE_SHAPE
        else:
            raise ValueError('Invalid shape string: %s' % shape_str)

        # Dict to store the results
        table = {}

        # Create a sub-block dict with shape 1x1x1x1x1
        sub_block_shape = {}
        for l in self.BGQ_DIMENSION_LABELS:
            sub_block_shape[l] = 1

        # Look over all the dimensions starting at the most right
        for dim in self.BGQ_MAPPING[::-1]:
            while True:

                # Calculate the number of nodes for the current shape
                from operator import mul
                num_nodes = reduce(mul, filter(lambda length: length != 0, sub_block_shape.values()))

                if num_nodes in self.BGQ_SUPPORTED_SUB_BLOCK_SIZES:
                    table[num_nodes] = copy.copy(sub_block_shape)
                else:
                    self._log.warning("Non supported sub-block size: %d.", num_nodes)

                # Done with iterating this dimension
                if sub_block_shape[dim] >= block_shape[dim]:
                    break

                # Increase the length in this dimension for the next iteration.
                if sub_block_shape[dim] == 1:
                    sub_block_shape[dim] = 2
                elif sub_block_shape[dim] == 2:
                    sub_block_shape[dim] = 4

        return table



