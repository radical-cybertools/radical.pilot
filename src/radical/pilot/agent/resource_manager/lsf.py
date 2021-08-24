
__copyright__ = 'Copyright 2018-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class LSF(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, log, prof):

        ResourceManager.__init__(self, cfg, log, prof)

    # --------------------------------------------------------------------------
    #
    def _update_info(self, info):

        lsf_hostfile = os.environ.get('LSB_DJOB_HOSTFILE')
        if not lsf_hostfile:
            raise RuntimeError('$LSB_DJOB_HOSTFILE not set')
        self._log.info('LSB_DJOB_HOSTFILE: %s', lsf_hostfile)

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
        # Count the cores while digging out the node names.
        lsf_nodes = dict()

        # LSF adds login and batch nodes to the hostfile (with 1 core) which
        # needs filtering out.
        with open(lsf_hostfile, 'r') as fin:
            for line in fin.readlines():
                if 'login' not in line and 'batch' not in line:
                    node = line.strip()
                    if node not in lsf_nodes: lsf_nodes[node]  = 1
                    else                    : lsf_nodes[node] += 1

        # It is possible that login/batch nodes were not marked at hostfile
        # and were not filtered out, thus we assume that there is only one
        # such node with 1 core (otherwise assertion error will be raised later)
        # *) affected machine(s): Lassen@LLNL
        for node, node_cores in lsf_nodes.items():
            if node_cores == 1:
                del lsf_nodes[node]
                break

        # RP currently requires uniform node configuration, so we expect the
        # same core count for all nodes
        assert(len(set(lsf_nodes.values())) == 1)
        info.cores_per_node = list(lsf_nodes.values())[0]
        self._log.debug('found %d nodes with %d cores',
                        len(lsf_nodes), info.cores_per_node)

        # ensure we can derive the number of cores per socket
        assert(not info.cores_per_node % info.sockets_per_node)
        info.cores_per_socket = int(info.cores_per_node / info.sockets_per_node)

        # same for gpus
        assert(not info.gpus_per_node % info.sockets_per_node)
        info.gpus_per_socket = int(info.gpus_per_node / info.sockets_per_node)

        # structure of the node list is
        #
        #   [[node_name_1, node_id_1],
        #    [node_name_2, node_id_2],
        #    ...
        #   ]
        #
        # While LSF node names are unique and could serve as node uids, we
        # need an integer index later on for resource set specifications.
        # (LSF starts node indexes at 1, not 0)
        info.node_list = [[name, str(idx + 1)]
                          for idx, name in enumerate(sorted(lsf_nodes.keys()))]

        return info


# ------------------------------------------------------------------------------

