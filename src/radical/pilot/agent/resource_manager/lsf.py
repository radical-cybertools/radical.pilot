
__copyright__ = 'Copyright 2018-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from ...constants import DOWN
from .base        import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class LSF(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        # LSF hostfile format:
        #
        #     node_1
        #     node_1
        #     ...
        #     node_2
        #     node_2
        #     ...
        #
        # There are in total "-n" entries (number of tasks of the job)
        # and "-R" entries per node (tasks per host).
        #
        hostfile = os.environ.get('LSB_DJOB_HOSTFILE')
        if not hostfile:
            raise RuntimeError('$LSB_DJOB_HOSTFILE not set')

        smt   = rm_info.threads_per_core
        nodes = self._parse_nodefile(hostfile, smt=smt)

        # LSF adds login and batch nodes to the hostfile (with 1 core) which
        # needs filtering out.
        #
        # It is possible that login/batch nodes were not marked at hostfile
        # and were not filtered out, thus we assume that there is only one
        # such node with 1 core (otherwise assertion error will be raised later)
        # *) affected machine(s): Lassen@LLNL
        filtered = list()
        for node in nodes:
            if   'login' in node[0]: continue
            elif 'batch' in node[0]: continue
            elif 1       == node[1]: continue
            filtered.append(node)

        nodes = filtered

        if not rm_info.cores_per_node:
            rm_info.cores_per_node = self._get_cores_per_node(nodes)

        self._log.debug('found %d nodes with %d cores',
                        len(nodes), rm_info.cores_per_node)

        # While LSF node names are unique and could serve as node uids, we
        # need an integer index later on for resource set specifications.
        # (LSF starts node indexes at 1, not 0)
        rm_info.node_list = self._get_node_list(nodes, rm_info)

        # Summit cannot address the last core of the second socket at the
        # moment, so we mark it as `DOWN` and the scheduler skips it.  We need
        # to check the SMT setting to make sure the right logical cores are
        # marked.  The error we see on those cores is: "ERF error: 1+ cpus are
        # not available"
        #
        # This is related to the known issue listed on
        #     https://www.olcf.ornl.gov/for-users/system-user-guides \
        #                              /summit/summit-user-guide/
        #     "jsrun explicit resource file (ERF) allocates incorrect resources"
        #
        if rm_info.cores_per_node > 40 and \
           'JSRUN' in self._cfg['resource_cfg']['launch_methods']:

            for node in rm_info.node_list:
                for i in range(smt):
                    node['cores'][21 * smt + i] = DOWN

            rm_info.cores_per_node -= 1


        # node uids need to be indexes starting at 1 in order to be usable for
        # jsrun ERF spec files
        for idx, node in enumerate(rm_info.node_list):
            node['node_id'] = str(idx + 1)

        return rm_info


# ------------------------------------------------------------------------------

