
__copyright__ = 'Copyright 2018-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class LSF(ResourceManager):

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def batch_started():

        return bool(os.getenv('LSB_JOBID'))

    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

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
        # such node with 1 physical core, i.e., equals to SMT threads
        # (otherwise assertion error will be raised later)
        # *) affected machine(s): Lassen@LLNL
        filtered = list()
        for node in nodes:
            if   'login' in node[0]: continue
            elif 'batch' in node[0]: continue
            elif smt     == node[1]: continue
            filtered.append(node)

        nodes = filtered

        lsf_cores_per_node = self._get_cores_per_node(nodes)
        if rm_info.cores_per_node:
            assert (rm_info.cores_per_node == lsf_cores_per_node)
        else:
            rm_info.cores_per_node = lsf_cores_per_node

        self._log.debug('found %d nodes with %d cores',
                        len(nodes), rm_info.cores_per_node)

        # While LSF node names are unique and could serve as node uids, we
        # need an integer index later on for resource set specifications.
        # (LSF starts node indexes at 1, not 0)
        rm_info.node_list = self._get_node_list(nodes, rm_info)

        # NOTE: blocked cores should be in sync with SMT level,
        #       as well with CPU indexing type ("logical" vs "physical").
        #       Example of CPU indexing on Summit:
        #          https://github.com/olcf-tutorials/ERF-CPU-Indexing
        #
        # The current approach uses "logical" CPU indexing
        # FIXME: set cpu_indexing as a parameter in resource config

        return rm_info


# ------------------------------------------------------------------------------

