
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import time

import radical.utils as ru

from .base import RMInfo, ResourceManager

from ...pilot_description import PilotDescription
from ...resource_config   import ResourceConfig


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
    @staticmethod
    def _inspect() -> PilotDescription:
        '''
        This method will inspect the local environment.  If it is determined
        that we are running in a Slurm job allocation, a suitable pilot
        description is crafted and returned.

        SLURM_CLUSTER_NAME=frontier
        SLURM_JOB_NUM_NODES=4
        SLURM_CPUS_ON_NODE=64
        SLURM_GPUS_ON_NODE=8
        SLURM_JOB_CPUS_PER_NODE=64(x4)
        SLURM_JOB_END_TIME=1712740416
        SLURM_JOB_START_TIME=1712740356
        SLURM_THREADS_PER_CORE=1
        '''

        if 'SLURM_CLUSTER_NAME' not in os.environ:
            raise RuntimeError('not running in a SLURM allocation')

        n_nodes  = int(os.environ['SLURM_JOB_NUM_NODES'])
        hostname =     os.environ['SLURM_CLUSTER_NAME']

        # we now have the hostname, but we need to know the resource label
        resource = None
        sites    = ru.Config('radical.pilot.resource', name='*', expand=False)
        for site in sites:
            if resource:
                break
            for res in sorted(sites[site].keys()):
                if hostname in res:
                    resource = '%s.%s' % (site, res)
                    break

        if not resource:
            raise RuntimeError('hostname %s not in resource config' % hostname)

        if not n_nodes:
            raise RuntimeError('SLURM_JOB_NUM_NODES not set')

        return PilotDescription(resource=resource, nodes=n_nodes)


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

