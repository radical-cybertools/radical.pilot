
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import math
import multiprocessing

import radical.utils as ru

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class Fork(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        ResourceManager.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._log.info("Using fork on localhost.")

        # For the fork ResourceManager (ie. on localhost), we fake an infinite
        # number of cores, so don't perform any sanity checks.
        detected_cores = multiprocessing.cpu_count()

        if detected_cores != self.requested_cores:
            if self._cfg.resource_cfg.fake_resources:
                self._log.info("using %d instead of available %d cores.",
                               self.requested_cores, detected_cores)
            else:
                if self.requested_cores > detected_cores:
                    raise RuntimeError('insufficient cores found (%d < %d'
                            % (detected_cores, self.requested_cores))

        # if cores_per_node is set in the agent config, we slice the number of
        # cores into that many virtual nodes.  cpn defaults to requested_cores,
        # to preserve the previous behavior (1 node).
        self.cores_per_node = self._cfg.get('cores_per_node', self.requested_cores)
        self.gpus_per_node  = self._cfg.get('gpus_per_node', 0)
        self.mem_per_node   = self._cfg.get('mem_per_node',  0)

        self.lfs_per_node   = {'path' : ru.expand_env(
                                           self._cfg.get('lfs_path_per_node')),
                               'size' :    self._cfg.get('lfs_size_per_node', 0)
                              }

        if not self.cores_per_node:
            self.cores_per_node = 1

        self.node_list = list()
        cpu_nodes      = int(math.ceil(float(self.requested_cores) /
                                       float(self.cores_per_node ) ) )
        if self.gpus_per_node:
            gpu_nodes       = int(math.ceil(float(self.requested_gpus) /
                                            float(self.gpus_per_node ) ) )
            requested_nodes = max(cpu_nodes, gpu_nodes)
        else:
            requested_nodes = cpu_nodes

        for i in range(requested_nodes):
            # enumerate the node list entries for a unique uis
            self.node_list.append(["localhost", 'localhost_%d' % i])

        self._log.debug('configure localhost as %s nodes '
                        '(%s cores, %s gpus, %s lfs, %s mem)',
                        len(self.node_list), self.cores_per_node,
                        self.gpus_per_node, self.lfs_per_node,
                        self.mem_per_node)


# ------------------------------------------------------------------------------

