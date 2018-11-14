
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

from base import LRMS


# ==============================================================================
#
class CCM(LRMS):
    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LRMS.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._log.info("Configured to run on system with %s.", self.name)

        CCM_NODEFILE_DIR = os.path.expanduser('~/.crayccm')

        ccm_nodefile_list = filter(lambda x: x.startswith('ccm_nodelist'),
                                   os.listdir(CCM_NODEFILE_DIR))
        if not ccm_nodefile_list:
            raise Exception("No CCM nodefiles found in: %s." % CCM_NODEFILE_DIR)

        ccm_nodefile_name = max(ccm_nodefile_list, key=lambda x:
                              os.stat(os.path.join(CCM_NODEFILE_DIR, x)).st_mtime)
        ccm_nodefile = os.path.join(CCM_NODEFILE_DIR, ccm_nodefile_name)

        hostname = os.uname()[1]
        if not hostname in open(ccm_nodefile).read():
            raise RuntimeError("Using the most recent CCM nodefile (%s),"
                               " but I (%s) am not in it!" % (ccm_nodefile, hostname))

        # Parse the CCM nodefile
        ccm_nodes = [line.strip() for line in open(ccm_nodefile)]
        self._log.info("Found CCM nodefile: %s.", ccm_nodefile)

        # Get the number of raw entries
        ccm_nodes_length = len(ccm_nodes)

        # Unique nodes
        ccm_node_list        = list(set(ccm_nodes))
        ccm_node_list_length = len(ccm_node_list)

        # Some simple arithmetic
        self.cores_per_node = ccm_nodes_length / ccm_node_list_length
        self.gpus_per_node  = self._cfg.get('gpus_per_node', 0) # FIXME GPU
        self.lfs_per_node   = {'path' : self._cfg.get('lfs_path_per_node', None),
                               'size' : self._cfg.get('lfs_size_per_node', 0)
                              }

        # node names are unique, so can serve as node uids
        self.node_list = [[node, node] for node in ccm_node_list]


# ------------------------------------------------------------------------------

