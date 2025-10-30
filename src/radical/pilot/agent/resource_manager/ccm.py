
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class CCM(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        # use the last (creation time) ccm_nodelist file in ~/.crayccm
        nodefile_dir  = os.path.expanduser('~/.crayccm')
        nodefile_list = [x for x in os.listdir(nodefile_dir)
                               if x.startswith('nodelist')]

        if not nodefile_list:
            raise Exception('no CCM nodefiles found in: %s' % nodefile_dir)

        nodefile_name = max(
            nodefile_list,
            key=lambda x: os.stat(os.path.join(nodefile_dir, x)).st_mtime)
        nodefile = os.path.join(nodefile_dir, nodefile_name)

        nodes = self._parse_nodefile(nodefile)

        if not rm_info.cores_per_node:
            rm_info.cores_per_node = self._get_cores_per_node(nodes)

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        return rm_info


# ------------------------------------------------------------------------------

