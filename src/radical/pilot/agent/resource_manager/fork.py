
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import multiprocessing

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class Fork(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _update_info(self, info):

        # check if the requested cores are available
        detected_cores = multiprocessing.cpu_count()

        if detected_cores != info.requested_cores:
            if self._cfg.resource_cfg.fake_resources:
                self._log.info(
                    'using %d requested cores instead of %d available cores',
                    info.requested_cores, detected_cores)
            else:
                if info.requested_cores > detected_cores:
                    raise RuntimeError('insufficient cores found (%d < %d)' %
                                       (detected_cores, info.requested_cores))

        # FIXME: number of GPUs should also be checked - how?
        # FIXME: we should use hwlock if available

        info.node_list = [['localhost', 'localhost_%d' % i]
                          for i in range(info.requested_nodes)]

        return info

# ------------------------------------------------------------------------------

