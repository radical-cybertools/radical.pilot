
__copyright__ = 'Copyright 2018-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class Debug(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        nodes = [('localhost', rm_info.cores_per_node)
                 for idx in range(rm_info.requested_nodes)]

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        # UIDs need to be made unique
        for idx, node in enumerate(rm_info.node_list):
            node['node_id'] = '%s_%04d' % (node['node_name'], idx)

        return rm_info


# ------------------------------------------------------------------------------

