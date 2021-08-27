
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class Slurm(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, log, prof):

        ResourceManager.__init__(self, cfg, log, prof)

    # --------------------------------------------------------------------------
    #
    def _update_info(self, info):

        slurm_nodelist = os.environ.get('SLURM_NODELIST')
        if slurm_nodelist is None:
            raise RuntimeError('$SLURM_NODELIST not set')

        # Parse SLURM nodefile environment variable
        slurm_nodes = ru.get_hostlist(slurm_nodelist)
        self._log.info('found SLURM_NODELIST %s. Expanded to: %s',
                       slurm_nodelist, slurm_nodes)

        # $SLURM_NPROCS = Total number of cores allocated for the current job
        slurm_nprocs_str = os.environ.get('SLURM_NPROCS')
        if slurm_nprocs_str is None:
            raise RuntimeError('$SLURM_NPROCS not set')
        slurm_nprocs = int(slurm_nprocs_str)

        # $SLURM_NNODES = Total number of (partial) nodes in
        # the job's resource allocation
        slurm_nnodes_str = os.environ.get('SLURM_NNODES')
        if slurm_nnodes_str is None:
            raise RuntimeError('$SLURM_NNODES not set')
        slurm_nnodes = int(slurm_nnodes_str)

        # $SLURM_CPUS_ON_NODE = Number of cores per node (physically)
        slurm_cpus_on_node_str = os.environ.get('SLURM_CPUS_ON_NODE')
        if slurm_cpus_on_node_str is None:
            raise RuntimeError('$SLURM_CPUS_ON_NODE not set')
        slurm_cpus_on_node = int(slurm_cpus_on_node_str)

        # Verify that $SLURM_NPROCS <= $SLURM_NNODES * $SLURM_CPUS_ON_NODE
        if slurm_nprocs > slurm_nnodes * slurm_cpus_on_node:
            self._log.warning('$SLURM_NPROCS(%d) > $SLURM_NNODES(%d) * ' +
                              '$SLURM_CPUS_ON_NODE(%d)',
                              slurm_nprocs, slurm_nnodes, slurm_cpus_on_node)

        # Verify that $SLURM_NNODES == len($SLURM_NODELIST)
        if slurm_nnodes != len(slurm_nodes):
            self._log.error('$SLURM_NNODES(%d) != len($SLURM_NODELIST)(%d)',
                            slurm_nnodes, len(slurm_nodes))

        if not info.cores_per_node:
            info.cores_per_node = min(slurm_cpus_on_node, slurm_nprocs)

        info.node_list = [[name, str(idx + 1)]
                          for idx, name in enumerate(sorted(slurm_nodes))]

        return info

# ------------------------------------------------------------------------------

