
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
    def init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

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

        if not rm_info.requested_nodes:
            rm_info.requested_nodes = int(
                math.ceil(rm_info.requested_cores / rm_info.cores_per_node))
            if rm_info.requested_gpus:
                rm_info.requested_nodes = max(
                    rm_info.requested_nodes,
                    int(math.ceil(rm_info.requested_gpus / rm_info.gpus_per_node)))

        n_nodes = rm_info.requested_nodes + rm_info.backup_nodes
        nodes   = [('localhost', rm_info.cores_per_node)
                   for _ in range(n_nodes)]

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        return rm_info


# ------------------------------------------------------------------------------

