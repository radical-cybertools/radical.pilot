
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.utils as ru

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class Cobalt(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        ResourceManager.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # we only support Cobalt on Theta right now, and since we know that
        # Theta is a Cray, we know that aprun is available.  Alas, aprun
        # provides the only way (we could find so far) to determing the list of
        # nodes we have available (`COBALT_NODELIST` seems broken).  So we run
        # `aprun` with the rank of nodes we *think* we have, and with `-N 1` to
        # place one rank per node, and run `hostname` - that gives is the list
        # of hostnames.  The number of nodes we receive from `$COBALT_PARTSIZE`.

        n_nodes          = int(os.environ['COBALT_PARTSIZE'])
        out, _, _        = ru.sh_callout('aprun -n %d -N 1 hostname' % n_nodes)
        node_list        = out.split()
        assert(len(node_list) == n_nodes), node_list

        # we also want    to learn the core count per node
        cmd              = 'cat /proc/cpuinfo | grep processor | wc -l'
        out, _, _        = ru.sh_callout('aprun -n %d -N 1 %s' % (n_nodes, cmd))
        core_counts      = list(set([int(x) for x in out.split()]))
        assert(len(core_counts) == 1), core_counts
        cores_per_node   = core_counts[0]

        gpus_per_node    = self._cfg.get('gpus_per_node', 0)
        lfs_per_node     = {'path': ru.expand_env(
                                       self._cfg.get('lfs_path_per_node')),
                            'size':    self._cfg.get('lfs_size_per_node', 0)
                           }
        mem_per_node     = self._cfg.get('mem_per_node', 0)

        self._log.info("Found unique core counts: %s", cores_per_node)

        # node names are unique, so can serve as node uids
        self.node_list        = [[node, node] for node in node_list]
        self.cores_per_node   = cores_per_node
        self.gpus_per_node    = gpus_per_node
        self.lfs_per_node     = lfs_per_node
        self.mem_per_node     = mem_per_node


# ------------------------------------------------------------------------------

