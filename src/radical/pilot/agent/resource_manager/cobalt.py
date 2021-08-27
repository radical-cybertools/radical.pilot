
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
    def __init__(self, cfg, log, prof):

        ResourceManager.__init__(self, cfg, log, prof)

    # --------------------------------------------------------------------------
    #
    def _update_info(self, info):

        try:
            # this env variable is used for GPU nodes
            cobalt_nodefile = os.environ['COBALT_NODEFILE']
            with open(cobalt_nodefile, 'r') as f:
                cobalt_nodes = [node.strip() for node in f.readlines() if node]
            self._log.info('COBALT_NODEFILE: %s', cobalt_nodefile)
        except KeyError as e:
            if 'COBALT_PARTNAME' not in os.environ:
                raise RuntimeError('$COBALT_PARTNAME not set') from e
            node_range   = os.environ['COBALT_PARTNAME']
            cobalt_nodes = ru.get_hostlist_by_range(node_range, 'nid', 5)

            # Another option is to run `aprun` with the rank of nodes
            # we *think* we have, and with `-N 1` to place one rank per node,
            # and run `hostname` - that gives the list of hostnames.
            # (The number of nodes we receive from `$COBALT_PARTSIZE`.)
            #   out = ru.sh_callout('aprun -q -n %d -N 1 hostname' % n_nodes)[0]
            #   node_names = out.split()

        info.node_list = [[name, str(idx + 1)]
                          for idx, name in enumerate(sorted(cobalt_nodes))]

        # get info about core count per node:
        #   cmd = 'cat /proc/cpuinfo | grep processor | wc -l'
        #   out = ru.sh_callout('aprun -q -n %d -N 1 %s' % (n_nodes, cmd))[0]
        #   core_counts = set([int(x) for x in out.split()])
        #   assert(len(core_counts) == 1), core_counts
        #   cores_per_node = core_counts[0]

        return info

# ------------------------------------------------------------------------------

