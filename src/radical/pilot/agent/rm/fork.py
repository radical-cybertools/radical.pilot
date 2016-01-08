
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import math
import multiprocessing

from .base import LRMS

# ==============================================================================
#
class Fork(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, logger):

        LRMS.__init__(self, cfg, logger)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._log.info("Using fork on localhost.")

        # For the fork LRMS (ie. on localhost), we fake an infinite number of
        # cores, so don't perform any sanity checks.
        detected_cpus = multiprocessing.cpu_count()

        if detected_cpus != self.requested_cores:
            self._log.info("using %d instead of physically available %d cores.",
                    self.requested_cores, detected_cpus)

        # if cores_per_node is set in the agent config, we slice the number of
        # cores into that many virtual nodes.  cpn defaults to requested_cores,
        # to preserve the previous behavior (1 node).
        self.cores_per_node = self._cfg.get('cores_per_node')
        if not self.cores_per_node:
            self.cores_per_node = self.requested_cores

        requested_nodes = int(math.ceil(float(self.requested_cores) / float(self.cores_per_node)))
        self.node_list  = list()
        for i in range(requested_nodes):
            self.node_list.append("localhost")

        self._log.debug('configure localhost to behave as %s nodes with %s cores each.',
                len(self.node_list), self.cores_per_node)



