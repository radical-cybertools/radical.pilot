
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class Torque(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _update_info(self, info):

        torque_nodefile = os.environ.get('PBS_NODEFILE')
        if torque_nodefile is None:
            raise RuntimeError('$PBS_NODEFILE not set')
        self._log.info('PBS_NODEFILE: %s', torque_nodefile)

        # Parse PBS the nodefile
        torque_nodes = [line.strip() for line in open(torque_nodefile)]

        # Number of cpus involved in allocation
        val = os.environ.get('PBS_NCPUS')
        if val:
            torque_num_cpus = int(val)
        else:
            torque_num_cpus = None
            self._log.warning('$PBS_NCPUS not set! (new Torque version?)')

        # Number of nodes involved in allocation
        val = os.environ.get('PBS_NUM_NODES')
        if val:
            torque_num_nodes = int(val)
        else:
            torque_num_nodes = None
            self._log.warning('$PBS_NUM_NODES not set! (old Torque version?)')

        # Number of cores (processors) per node
        val = os.environ.get('PBS_NUM_PPN')
        if val:
            torque_cores_per_node = int(val)
        else:
            torque_cores_per_node = None
            self._log.warning('$PBS_NUM_PPN is not set!')

        saga_ppn = os.environ.get('SAGA_PPN')
        if torque_cores_per_node in [None, 1] and saga_ppn:
            # lets see if SAGA has been forthcoming with some information
            self._log.warning('fall back to $SAGA_PPN: %s', saga_ppn)
            torque_cores_per_node = int(saga_ppn)

        # Number of entries in nodefile should be PBS_NUM_NODES * PBS_NUM_PPN
        torque_nodes_length = len(torque_nodes)
        # only unique node names
        torque_nodes_uniq        = list(set(torque_nodes))
        torque_nodes_uniq_length = len(torque_nodes_uniq)

      # if torque_num_nodes and torque_cores_per_node and \
      #     torque_nodes_length < torque_num_nodes * torque_cores_per_node:
      #     raise RuntimeError(
      #         'Number of entries in $PBS_NODEFILE (%s) does not match with '
      #         '$PBS_NUM_NODES*$PBS_NUM_PPN (%s*%s)' % \
      #         (torque_nodes_length, torque_num_nodes,  torque_cores_per_node))

        if not info.cores_per_node:
            if torque_num_nodes and torque_cores_per_node:
                # Modern style Torque
                info.cores_per_node = torque_cores_per_node
            elif torque_num_cpus:
                # Blacklight style (TORQUE-2.3.13)
                info.cores_per_node = torque_num_cpus
            else:
                # Old style Torque (Should we just use this for all versions?)
                info.cores_per_node = torque_nodes_length \
                                      / torque_nodes_uniq_length

        info.node_list = [[name, str(idx + 1)]
                          for idx, name in enumerate(sorted(torque_nodes_uniq))]

        return info

# ------------------------------------------------------------------------------

