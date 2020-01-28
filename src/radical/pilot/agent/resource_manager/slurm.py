
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import hostlist

import radical.utils as ru

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class Slurm(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        ResourceManager.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        slurm_nodelist = os.environ.get('SLURM_NODELIST')
        if slurm_nodelist is None:
            msg = "$SLURM_NODELIST not set!"
            self._log.error(msg)
            raise RuntimeError(msg)

        # Parse SLURM nodefile environment variable
        slurm_nodes = hostlist.expand_hostlist(slurm_nodelist)
        self._log.info("Found SLURM_NODELIST %s. Expanded to: %s", slurm_nodelist, slurm_nodes)

        # $SLURM_NPROCS = Total number of cores allocated for the current job
        slurm_nprocs_str = os.environ.get('SLURM_NPROCS')
        if slurm_nprocs_str is None:
            msg = "$SLURM_NPROCS not set!"
            self._log.error(msg)
            raise RuntimeError(msg)
        else:
            slurm_nprocs = int(slurm_nprocs_str)

        # $SLURM_NNODES = Total number of (partial) nodes in the job's resource allocation
        slurm_nnodes_str = os.environ.get('SLURM_NNODES')
        if slurm_nnodes_str is None:
            msg = "$SLURM_NNODES not set!"
            self._log.error(msg)
            raise RuntimeError(msg)
        else:
            slurm_nnodes = int(slurm_nnodes_str)

        # $SLURM_CPUS_ON_NODE = Number of cores per node (physically)
        slurm_cpus_on_node_str = os.environ.get('SLURM_CPUS_ON_NODE')
        if slurm_cpus_on_node_str is None:
            msg = "$SLURM_CPUS_ON_NODE not set!"
            self._log.error(msg)
            raise RuntimeError(msg)
        else:
            slurm_cpus_on_node = int(slurm_cpus_on_node_str)

        # Verify that $SLURM_NPROCS <= $SLURM_NNODES * $SLURM_CPUS_ON_NODE
        if not slurm_nprocs <= slurm_nnodes * slurm_cpus_on_node:
            self._log.warning("$SLURM_NPROCS(%d) <= $SLURM_NNODES(%d) * $SLURM_CPUS_ON_NODE(%d)",
                            slurm_nprocs, slurm_nnodes, slurm_cpus_on_node)

        # Verify that $SLURM_NNODES == len($SLURM_NODELIST)
        if slurm_nnodes != len(slurm_nodes):
            self._log.error("$SLURM_NNODES(%d) != len($SLURM_NODELIST)(%d)",
                           slurm_nnodes, len(slurm_nodes))

        # Report the physical number of cores or the total number of cores
        # in case of a single partial node allocation.
        self.cores_per_node = self._cfg.get('cores_per_node', 0)
        self.gpus_per_node  = self._cfg.get('gpus_per_node',  0)  # FIXME GPU
        self.mem_per_node   = self._cfg.get('mem_per_node',   0)

        self.lfs_per_node   = {'path' : ru.expand_env(
                                           self._cfg.get('lfs_path_per_node')),
                               'size' :    self._cfg.get('lfs_size_per_node', 0)
                              }

        if not self.cores_per_node:
            self.cores_per_node = min(slurm_cpus_on_node, slurm_nprocs)


        # node names are unique, so can serve as node uids
        self.node_list = [[node, node] for node in slurm_nodes]

        self.lm_info['cores_per_node'] = self.cores_per_node

      # # once we are done, we remove all `SLURM_*` and `SBATCH_*` env variables
      # # as to not confuse srun executions
      # for k in list(os.environ.keys()):
      #     if k.startswith('SLURM_') or k.startswith('SBATCH_'):
      #         print 'unset %s' % k
      #         del(os.environ[k])


# ------------------------------------------------------------------------------

