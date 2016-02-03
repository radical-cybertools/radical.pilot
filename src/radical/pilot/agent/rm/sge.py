
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

from base import LRMS


# ==============================================================================
#
class SGE(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, logger):

        LRMS.__init__(self, cfg, logger)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        sge_hostfile = os.environ.get('PE_HOSTFILE')
        if sge_hostfile is None:
            msg = "$PE_HOSTFILE not set!"
            self._log.error(msg)
            raise RuntimeError(msg)

        # SGE core configuration might be different than what multiprocessing
        # announces
        # Alternative: "qconf -sq all.q|awk '/^slots *[0-9]+$/{print $2}'"

        # Parse SGE hostfile for nodes
        sge_node_list = [line.split()[0] for line in open(sge_hostfile)]
        # Keep only unique nodes
        sge_nodes = list(set(sge_node_list))
        self._log.info("Found PE_HOSTFILE %s. Expanded to: %s", sge_hostfile, sge_nodes)

        # Parse SGE hostfile for cores
        sge_cores_count_list = [int(line.split()[1]) for line in open(sge_hostfile)]
        sge_core_counts = list(set(sge_cores_count_list))
        sge_cores_per_node = min(sge_core_counts)
        self._log.info("Found unique core counts: %s Using: %d", sge_core_counts, sge_cores_per_node)

        self.node_list = sge_nodes
        self.cores_per_node = sge_cores_per_node



