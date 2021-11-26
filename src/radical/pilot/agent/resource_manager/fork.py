
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import multiprocessing

import math

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class Fork(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        # check if the requested cores are available
        detected_cores = multiprocessing.cpu_count()

        if detected_cores != rm_info.requested_cores:
            if self._cfg.resource_cfg.fake_resources:
                self._log.info(
                    'using %d requested cores instead of %d available cores',
                    rm_info.requested_cores, detected_cores)
            else:
                if detected_cores < rm_info.requested_cores:
                    raise RuntimeError('insufficient cores found (%d < %d)' %
                                       (detected_cores, rm_info.requested_cores))

        if not rm_info.requested_nodes:
            cores = rm_info.requested_cores
            cpn   = rm_info.cores_per_node
            rm_info.requested_nodes = math.ceil(cores / cpn)

        # FIXME: number of GPUs should also be checked - how? Should we use
        #        hwlock if available?

        nodes = [('localhost', rm_info.cores_per_node)
                 for idx in range(rm_info.requested_nodes)]

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        # UIDs need to be made unique
        for idx, node in enumerate(rm_info.node_list):
            node['node_id'] = '%s_%04d' % (node['node_name'], idx)

        return rm_info


# ------------------------------------------------------------------------------

