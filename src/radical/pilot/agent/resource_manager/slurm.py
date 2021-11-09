
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class Slurm(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        nodelist = os.environ.get('SLURM_NODELIST')
        if nodelist is None:
            raise RuntimeError('$SLURM_NODELIST not set')

        # Parse SLURM nodefile environment variable
        node_names = ru.get_hostlist(nodelist)
        self._log.info('found SLURM_NODELIST %s. Expanded to: %s',
                       nodelist, node_names)

        if not rm_info.cores_per_node:
            # $SLURM_CPUS_ON_NODE = Number of physical cores per node
            cpn_str = os.environ.get('SLURM_CPUS_ON_NODE')
            if cpn_str is None:
                raise RuntimeError('$SLURM_CPUS_ON_NODE not set')
            rm_info.cores_per_node = int(cpn_str)

        nodes = [(node, rm_info.cores_per_node) for node in node_names]

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        return rm_info


# ------------------------------------------------------------------------------

