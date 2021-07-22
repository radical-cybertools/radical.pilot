
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import math
import multiprocessing

import radical.utils as ru

from .base import ResourceManager, RMInfo


# ------------------------------------------------------------------------------
#
class Fork(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, log, prof):

        ResourceManager.__init__(self, cfg, log, prof)


    # --------------------------------------------------------------------------
    #
    def _configure(self, info):

        self._log.info("Using fork on localhost.")

        # check if the requested cores are available
        detected_cores = multiprocessing.cpu_count()

        if detected_cores != info.requested_cores:
            if self._cfg.resource_cfg.fake_resources:
                self._log.info("using %d instead of available %d cores.",
                               info.requested_cores, detected_cores)
            else:
                if info.requested_cores > detected_cores:
                    raise RuntimeError('insufficient cores found (%d < %d'
                            % (detected_cores, info.requested_cores))

        # FIXME: number of GPUs should also be checked - how?
        # FIXME: we should use hwlock if available

        info.node_list = [["localhost", 'localhost_%d' % i]
                          for i in range(info.requested_nodes)]

        return info


# ------------------------------------------------------------------------------

