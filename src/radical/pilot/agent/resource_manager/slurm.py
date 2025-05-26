
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from rc.process import Process

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

        # filter out nodes which are not accessible.  Test access by running
        # `ssh <node> hostname` and checking the return code.
        procs = list()
        for node in rm_info.node_list:
            name = node['name']
            cmd  = 'ssh -oBatchMode=yes %s hostname' % name
            self._log.debug('check node: %s [%s]', name, cmd)
            proc = Process(cmd)
            proc.start()
            procs.append([name, proc, node])

        ok = list()
        for name, proc, node in procs:
            proc.wait(timeout=15)
            self._log.debug('check node: %s [%s]', name,
                            [proc.stdout, proc.stderr, proc.retcode])
            if proc.retcode is not None:
                if not proc.retcode:
                    ok.append(node)
            else:
                self._log.warning('check node: %s [%s] timed out',
                                  name, [proc.stdout, proc.stderr])
                proc.cancel()
                proc.wait(timeout=15)
                if proc.retcode is None:
                    self._log.warning('check node: %s [%s] timed out again',
                                       name, [proc.stdout, proc.stderr])

        self._log.warning('using %d nodes out of %d', len(ok), len(nodes))

        if not ok:
            raise RuntimeError('no accessible nodes found')

        # limit the nodelist to the requested number of nodes

        rm_info.node_list = ok



        return rm_info


# ------------------------------------------------------------------------------

