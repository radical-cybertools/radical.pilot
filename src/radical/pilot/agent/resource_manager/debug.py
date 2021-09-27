
__copyright__ = 'Copyright 2018-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class Debug(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _update_info(self, info):

        info.node_list = [['node_%d' % idx, str(idx + 1)]
                          for idx in range(info.requested_nodes)]

        return info

# ------------------------------------------------------------------------------

