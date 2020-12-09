
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

        try:
            with open(os.environ['COBALT_NODEFILE'], 'r') as f:
                node_list = [node.strip() for node in f.readlines() if node]
        except KeyError:
            n_nodes = int(os.environ['COBALT_PARTSIZE'])
            node_list = ['node_%04d' % i for i in range(n_nodes)]

            # Another option is to run `aprun` with the rank of nodes
            # we *think* we have, and with `-N 1` to place one rank per node,
            # and run `hostname` - that gives the list of hostnames.
            # The number of nodes we receive from `$COBALT_PARTSIZE`.
          # out, _, _ = ru.sh_callout('aprun -q -n %d -N 1 hostname' % n_nodes)
          # node_list = out.split()

      # # we also want to learn the core count per node
      # cmd              = 'cat /proc/cpuinfo | grep processor | wc -l'
      # out, _, _        = ru.sh_callout('aprun -q -n %d -N 1 %s' % (n_nodes, cmd))
      # core_counts      = set([int(x) for x in out.split()])
      # print('=== out 2 : [%s] [%s]' % (out, core_counts))
      # assert(len(core_counts) == 1), core_counts
      # cores_per_node   = core_counts[0]

        # node names are unique, so can serve as node uids
        self.node_list      = [[node, node] for node in node_list]
        self.cores_per_node = self._cfg.get('cores_per_node', 1)
        self.gpus_per_node  = self._cfg.get('gpus_per_node', 0)
        self.mem_per_node   = self._cfg.get('mem_per_node', 0)
        self.lfs_per_node   = {'path': ru.expand_env(
                                       self._cfg.get('lfs_path_per_node')),
                               'size': self._cfg.get('lfs_size_per_node', 0)}

# ------------------------------------------------------------------------------
