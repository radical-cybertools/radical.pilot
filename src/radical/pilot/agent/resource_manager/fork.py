
__copyright__ = 'Copyright 2016-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import math
import multiprocessing

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class Fork(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        detected_cores = multiprocessing.cpu_count()
        if not rm_info.cores_per_node:
            rm_info.cores_per_node = detected_cores

        if self._rcfg.fake_resources:
            self._log.info(
                'virtual resource with %d cores per node (%d detected cores)' %
                (rm_info.cores_per_node, detected_cores))
        elif detected_cores >= rm_info.requested_cores:
            if rm_info.cores_per_node > detected_cores:
                self._log.warn(
                    'defined %d cores per node > %d detected cores' %
                    (rm_info.cores_per_node, detected_cores))
            elif rm_info.cores_per_node < rm_info.requested_cores:
                raise RuntimeError(
                    'incorrectly defined %d cores per node for real resource,'
                    'while requesting %d cores' %
                    (rm_info.cores_per_node, rm_info.requested_cores))
        else:
            raise RuntimeError(
                'insufficient cores found (%d < %d)' %
                (detected_cores, rm_info.requested_cores))

        # duplicates the code from the base class, but here it should be
        # called first before determining `rm_info.node_list`
        if not rm_info.requested_nodes:
            n_nodes = rm_info.requested_cores / rm_info.cores_per_node
            rm_info.requested_nodes = math.ceil(n_nodes)
            # FIXME: number of GPUs should also be checked - how?
            #        Should we use hwlock if available?

        nodes = [('localhost', rm_info.cores_per_node)
                 for _ in range(rm_info.requested_nodes)]

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        return rm_info


# ------------------------------------------------------------------------------

