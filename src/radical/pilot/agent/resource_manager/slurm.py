
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


    @staticmethod
    def partition_env(n_parts: int) -> dict:

        parts = {x: dict() for x in range(n_parts)}

        # search for `SLURM_*` env values.  Copy them into separate dicts (one
        # per partition).  All env's which related to the nodelist (number of
        # nodes, name of nodes etc) need to be changed so that the nodes are
        # distributed evenly across the partitions.
        #
        # NOTE: we remove all `SLURM_STEP_*` envs
        #
        n_nodes = int(os.environ.get('SLURM_NNODES', 0))
        p_nodes = n_nodes / n_parts

        if not n_nodes:
            # no nodes, no partitions
            raise RuntimeError('SLURM_NNODES not set')

        assert (p_nodes * n_parts) == n_nodes, \
                'n_nodes (%d) not divisible by n_parts (%d)' % (n_nodes, n_parts)

        # for all partitions, create a copy of the environment
        for key,val in os.environ.items():

            # we remove all job step variables
            if key.startswith('SLURM_STEP_'):
                pass

            #  partitioning: expand nodelist, partition it, compress partitions
            elif key in ['SLURM_JOB_NODELIST', 'SLURM_NODELIST']:

                nodes = ru.get_hostlist(val)
                part_nodes = ru.partition(nodes, n_parts)

                for i in range(n_parts):
                    comp = ru.compress_hostlist(part_nodes[i])
                    full = ru.get_hostlist(comp)

                    if full != part_nodes[i]:
                        with open('nl_full.txt', 'w') as f:
                            f.write('\n'.join(full))
                        with open('nl_part.txt', 'w') as f:
                            f.write('\n'.join(part_nodes[i]))
                        with open('nl_comp.txt', 'w') as f:
                            f.write(comp)
                        raise ValueError('nodelist does not match partitioned nodes')

                    parts[i][key] = ru.compress_hostlist(part_nodes[i])


            # some SLURM envs need to reflect the partition size
            elif key in ['SLURM_JOB_NUM_NODES', 'SLURM_NNODES',
                         'SLURM_NPROCS', 'SLURM_NTASKS']:

                ival = int(int(val) / n_parts)
                for i in range(n_parts):
                    parts[i][key] = str(ival)


            # dito, but specific to the number of cores per node
            # SLURM_JOB_CPUS_PER_NODE=56(x16) ->
            # SLURM_JOB_CPUS_PER_NODE=56(x8)
            elif '(x%d)' % n_nodes in val:

                val = val.replace('(x%d)' % n_nodes, '(x%d)' % p_nodes)
                for i in range(n_parts):
                    parts[i][key] = val

            # all other envs are copied unchanged
            else:
                for i in range(n_parts):
                    parts[i][key] = val


        return parts


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

