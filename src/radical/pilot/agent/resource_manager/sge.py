
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import signal

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class SGE(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _update_info(self, info):

        sge_hostfile = os.environ.get('PE_HOSTFILE')
        if sge_hostfile is None:
            raise RuntimeError('$PE_HOSTFILE not set')
        self._log.info('PE_HOSTFILE: %s', sge_hostfile)

        def _sigusr2_handler():
            self._log.warn('caught sigusr2')
            self.stop()

        signal.signal(signal.SIGUSR1, _sigusr2_handler)

        # SGE core configuration might be different than what multiprocessing
        # announces
        # Alternative: "qconf -sq all.q|awk '/^slots *[0-9]+$/{print $2}'"

        # Parse SGE hostfile for nodes
        sge_node_list = [line.split()[0] for line in open(sge_hostfile)]

        # Parse SGE hostfile for cores
        sge_cores_count_list = [int(line.split()[1]) for line in open(sge_hostfile)]
        sge_core_counts      = list(set(sge_cores_count_list))

        # Check if nodes have the same core count
        if len(sge_core_counts) == 1:
            sge_cores_per_node = min(sge_core_counts)
            info.node_list = [[name, str(idx + 1)]
                              for idx, name in enumerate(sorted(sge_node_list))]
        else:
            # Non-homogeneous counts: consider all slots be single core
            sge_cores_per_node = 1
            # Expand node list, create unique IDs for each core
            self.node_list = []
            for node, cores in zip(sge_node_list, sge_cores_count_list):
                for core in range(cores):
                    self.node_list.append([node, '%s_%s' % (node, core)])

        self._log.info('Found unique core counts: %s; Using: %d',
                       sge_core_counts, sge_cores_per_node)
        info.cores_per_node = sge_cores_per_node

        return info

# ------------------------------------------------------------------------------

