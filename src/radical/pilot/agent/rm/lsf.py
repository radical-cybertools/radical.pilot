
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

from base import LRMS


# ==============================================================================
#
class LSF(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LRMS.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        lsf_hostfile = os.environ.get('LSB_DJOB_HOSTFILE')
        if lsf_hostfile is None:
            msg = "$LSB_DJOB_HOSTFILE not set!"
            self._log.error(msg)
            raise RuntimeError(msg)

        lsb_mcpu_hosts = os.environ.get('LSB_MCPU_HOSTS')
        if lsb_mcpu_hosts is None:
            msg = "$LSB_MCPU_HOSTS not set!"
            self._log.error(msg)
            raise RuntimeError(msg)

        # parse LSF hostfile
        # format:
        # <hostnameX>
        # <hostnameX>
        # <hostnameY>
        # <hostnameY>
        #
        # There are in total "-n" entries (number of tasks)
        # and "-R" entries per host (tasks per host).
        # (That results in "-n" / "-R" unique hosts)
        #
        lsf_nodes = [line.strip() for line in open(lsf_hostfile)]
        self._log.info("Found LSB_DJOB_HOSTFILE %s. Expanded to: %s",
                      lsf_hostfile, lsf_nodes)
        lsf_node_list = list(set(lsf_nodes))

        # Grab the core (slot) count from the environment
        # Format: hostX N hostY N hostZ N
        lsf_cores_count_list = map(int, lsb_mcpu_hosts.split()[1::2])
        lsf_core_counts      = list(set(lsf_cores_count_list))
        lsf_cores_per_node   = min(lsf_core_counts)
        lsf_gpus_per_node    = self._cfg.get('gpus_per_node', 0) # FIXME GPU
        lfs_lfs_per_node     = {'path' : self._cfg.get('lfs_path_per_node', None),
                                'size' : self._cfg.get('lfs_size_per_node', 0)
                               }

        self._log.info("Found unique core counts: %s Using: %d",
                      lsf_core_counts, lsf_cores_per_node)

        # node names are unique, so can serve as node uids
        self.node_list      = [[node, node] for node in lsf_node_list]
        self.cores_per_node = lsf_cores_per_node
        self.gpus_per_node  = lsf_gpus_per_node
        self.lfs_per_node   = lfs_lfs_per_node


