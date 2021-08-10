
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

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
            # this env variable is used for GPU nodes
            with open(os.environ['COBALT_NODEFILE'], 'r') as f:
                node_names = [node.strip() for node in f.readlines() if node]
        except KeyError:
            node_range = os.environ['COBALT_PARTNAME']
            node_names = ru.get_hostlist_by_range(node_range, 'nid', 5)

            # Another option is to run `aprun` with the rank of nodes
            # we *think* we have, and with `-N 1` to place one rank per node,
            # and run `hostname` - that gives the list of hostnames.
            # (The number of nodes we receive from `$COBALT_PARTSIZE`.)
            #   out = ru.sh_callout('aprun -q -n %d -N 1 hostname' % n_nodes)[0]
            #   node_names = out.split()

        self.node_list = [[n, str(i + 1)] for i, n in enumerate(node_names)]

        # get info about core count per node:
        #   cmd = 'cat /proc/cpuinfo | grep processor | wc -l'
        #   out = ru.sh_callout('aprun -q -n %d -N 1 %s' % (n_nodes, cmd))[0]
        #   core_counts = set([int(x) for x in out.split()])
        #   print('=== out 2 : [%s] [%s]' % (out, core_counts))
        #   assert(len(core_counts) == 1), core_counts
        #   cores_per_node = core_counts[0]

        self.cores_per_node = self._cfg.get('cores_per_node', 1)
        self.gpus_per_node  = self._cfg.get('gpus_per_node', 0)
        self.mem_per_node   = self._cfg.get('mem_per_node', 0)
        self.lfs_per_node   = {'path': ru.expand_env(
                                       self._cfg.get('lfs_path_per_node')),
                               'size': self._cfg.get('lfs_size_per_node', 0)}

# ------------------------------------------------------------------------------
