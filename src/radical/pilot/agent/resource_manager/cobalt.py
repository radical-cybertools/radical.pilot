
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class Cobalt(ResourceManager):

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def batch_started():

        return bool(os.getenv('COBALT_JOBID'))

    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        if not rm_info.cores_per_node:
            raise RuntimeError('cores_per_node undetermined')

        if 'COBALT_NODEFILE' in os.environ:

            # this env variable is used for GPU nodes
            nodefile = os.environ['COBALT_NODEFILE']
            nodes    = self._parse_nodefile(nodefile, rm_info.cores_per_node)

        elif 'COBALT_PARTNAME' in os.environ:

            node_range = os.environ['COBALT_PARTNAME']
            nodes = [(node, rm_info.cores_per_node)
                     for node in ru.get_hostlist_by_range(node_range, 'nid', 5)]

            # Another option is to run `aprun` with the rank of nodes
            # we *think* we have, and with `-N 1` to place one rank per node,
            # and run `hostname` - that gives the list of hostnames.
            # (The number of nodes we receive from `$COBALT_PARTSIZE`.)
            #   out = ru.sh_callout('aprun -q -n %d -N 1 hostname' % n_nodes)[0]
            #   nodes = out.split()

        else:
            raise RuntimeError('no $COBALT_NODEFILE nor $COBALT_PARTNAME set')

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        return rm_info


# ------------------------------------------------------------------------------

