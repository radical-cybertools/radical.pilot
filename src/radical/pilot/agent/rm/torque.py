
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.utils as ru

from base import LRMS


# ==============================================================================
#
class Torque(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LRMS.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._log.info("Configured to run on system with %s.", self.name)

        torque_nodefile = os.environ.get('PBS_NODEFILE')
        if torque_nodefile is None:
            msg = "$PBS_NODEFILE not set!"
            self._log.error(msg)
            raise RuntimeError(msg)

        # Parse PBS the nodefile
        torque_nodes = [line.strip() for line in open(torque_nodefile)]
        self._log.info("PBS_NODEFILE %s: %s", torque_nodefile, torque_nodes)

        # Number of cpus involved in allocation
        val = os.environ.get('PBS_NCPUS')
        if val:
            torque_num_cpus = int(val)
        else:
            msg = "$PBS_NCPUS not set! (new Torque version?)"
            torque_num_cpus = None
            self._log.warning(msg)

        # Number of nodes involved in allocation
        val = os.environ.get('PBS_NUM_NODES')
        if val:
            torque_num_nodes = int(val)
        else:
            msg = "$PBS_NUM_NODES not set! (old Torque version?)"
            torque_num_nodes = None
            self._log.warning(msg)

        torque_gpus_per_node  = self._cfg.get('gpus_per_node', 0)
        torque_mem_per_node   = self._cfg.get('mem_per_node',  0)
        torque_lfs_per_node   = {'path': self._cfg.get('lfs_path_per_node', ''),
                                 'size': self._cfg.get('lfs_size_per_node', 0)}

        # Number of cores (processors) per node
        val = os.environ.get('PBS_NUM_PPN')
        if val:
            torque_cores_per_node = int(val)
        else:
            msg = "$PBS_NUM_PPN is not set!"
            torque_cores_per_node = None
            self._log.warning(msg)

        if self._cfg.get('cores_per_node'):
            cfg_cpn = self._cfg.get('cores_per_node')
            self._log.info('overwriting cores_per_node[%s] from cfg [%s]', 
                    torque_cores_per_node, cfg_cpn)
            torque_cores_per_node = cfg_cpn


        if torque_cores_per_node in [None, 1]:
            # lets see if SAGA has been forthcoming with some information
            saga_ppn = os.environ.get('SAGA_PPN')
            if saga_ppn:
                self._log.warning("fall back to $SAGA_PPN: %s", saga_ppn)
                torque_cores_per_node = int(saga_ppn)

        # Number of entries in nodefile should be PBS_NUM_NODES * PBS_NUM_PPN
        torque_nodes_length = len(torque_nodes)
        torque_node_list    = list(set(torque_nodes))

      # if torque_num_nodes and torque_cores_per_node and \
      #     torque_nodes_length < torque_num_nodes * torque_cores_per_node:
      #     raise RuntimeError("len($PBS_NODEFILE (%s)) " % torque_nodes_length
      #                      + " != $PBS_NUM_NODES*$PBS_NUM_PPN (%s*%s)" % 
      #                        (torque_num_nodes,  torque_cores_per_node))

        # only unique node names
        torque_node_list_length = len(torque_node_list)
        self._log.debug("Node list: %s(%d)",
                        torque_node_list, torque_node_list_length)

        if torque_num_nodes and torque_cores_per_node:
            # Modern style Torque
            self.cores_per_node = torque_cores_per_node
        elif torque_num_cpus:
            # Blacklight style (TORQUE-2.3.13)
            self.cores_per_node = torque_num_cpus
        else:
            # Old style Torque (Should we just use this for all versions?)
            self.cores_per_node = torque_nodes_length / torque_node_list_length

        # node names are unique, so can serve as node uids
        self.node_list     = [[node, node] for node in torque_node_list]
        self.gpus_per_node = torque_gpus_per_node
        self.mem_per_node  = torque_mem_per_node
        self.lfs_per_node  = torque_lfs_per_node


# ------------------------------------------------------------------------------

