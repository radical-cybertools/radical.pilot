
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class Slurm(ResourceManager):

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def batch_started():

        return bool(os.getenv('SLURM_JOB_ID'))

    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        ru.write_json(rm_info, 'rm_info.json')

        nodelist = os.environ.get('SLURM_NODELIST') or \
                   os.environ.get('SLURM_JOB_NODELIST')
        if nodelist is None:
            raise RuntimeError('$SLURM_*NODELIST not set')

        # Parse SLURM nodefile environment variable
        node_names = ru.get_hostlist(nodelist)
        self._log.info('found nodelist %s. Expanded to: %s',
                       nodelist, node_names)

        if not rm_info.cores_per_node:
            # $SLURM_CPUS_ON_NODE = Number of physical cores per node
            cpn_str = os.environ.get('SLURM_CPUS_ON_NODE')
            if cpn_str is None:
                raise RuntimeError('$SLURM_CPUS_ON_NODE not set')
            rm_info.cores_per_node = int(cpn_str)

        if not rm_info.gpus_per_node:
            if os.environ.get('SLURM_GPUS_ON_NODE'):
                rm_info.gpus_per_node = int(os.environ['SLURM_GPUS_ON_NODE'])
            else:
                # GPU IDs per node
                # - global context: SLURM_JOB_GPUS and SLURM_STEP_GPUS
                # - cgroup context: GPU_DEVICE_ORDINAL
                gpu_ids = os.environ.get('SLURM_JOB_GPUS')  or \
                          os.environ.get('SLURM_STEP_GPUS') or \
                          os.environ.get('GPU_DEVICE_ORDINAL')
                if gpu_ids:
                    rm_info.gpus_per_node = len(gpu_ids.split(','))

        nodes = [(node, rm_info.cores_per_node) for node in node_names]

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        return rm_info


# ------------------------------------------------------------------------------

