
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class CCM(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, log, prof):

        ResourceManager.__init__(self, cfg, log, prof)

    # --------------------------------------------------------------------------
    #
    def _update_info(self, info):

        ccm_nodefile_dir  = os.path.expanduser('~/.crayccm')
        ccm_nodefile_list = [x for x in os.listdir(ccm_nodefile_dir)
                             if x.startswith('ccm_nodelist')]

        if not ccm_nodefile_list:
            raise Exception('no CCM nodefiles found in: %s' % ccm_nodefile_dir)

        ccm_nodefile_name = max(
            ccm_nodefile_list,
            key=lambda x: os.stat(os.path.join(ccm_nodefile_dir, x)).st_mtime)
        ccm_nodefile = os.path.join(ccm_nodefile_dir, ccm_nodefile_name)

        hostname = os.uname()[1]
        if hostname not in open(ccm_nodefile).read():
            raise RuntimeError(
                'Using the most recent CCM nodefile (%s), ' % ccm_nodefile +
                'but I (%s) am not in it!'                  % hostname)

        # Parse the CCM nodefile
        ccm_nodes = [line.strip() for line in open(ccm_nodefile)]
        self._log.info('found CCM nodefile: %s', ccm_nodefile)

        # Get the number of raw entries
        ccm_nodes_length = len(ccm_nodes)

        # Unique nodes
        ccm_node_list        = list(set(ccm_nodes))
        ccm_node_list_length = len(ccm_node_list)

        # Some simple arithmetic
        info.cores_per_node = ccm_nodes_length / ccm_node_list_length

        # node names are unique, so can serve as node uids
        info.node_list = [[name, str(idx + 1)]
                          for idx, name in enumerate(sorted(ccm_node_list))]

        return info

# ------------------------------------------------------------------------------

