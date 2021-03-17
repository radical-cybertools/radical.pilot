# pylint: disable=super-init-not-called

__copyright__ = 'Copyright 2018-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import pprint

import radical.utils  as ru
from ... import agent as rpa

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class LSF_SUMMIT(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        # We temporarily do not call the base class constructor. The
        # constraint was not to change the base class at any point.
        # The constructor of the base class performs certain computations
        # that are specific to a node architecture, i.e., (i) requirement of
        # cores_per_node and gpus_per_node, (ii) no requirement for
        # sockets_per_node, and (iii) no validity checks on cores_per_socket,
        # gpus_per_socket, and sockets_per_node. It is, hence, incompatible
        # with the node architecture expected within this module.

        # We have three options:
        # 1) Change the child class, do not call the base class constructor
        # 2) Call the base class constructor, make the child class and its node
        #    structure compatible with that expected in the base class.
        # 3) Change the base class --- Out of scope of this project

        # 3 is probably the correct approach, but long term. 2 is not a
        # good approach as we are striving to keep a child class compatible
        # with a base class (this should never be the case).
        # We go ahead with 1, process of elimination really, but with the
        # advantage that we have the code content that will be required when
        # we implement 3, the long term approach.

        # ResourceManager.__init__(self, cfg, session)

        self.name            = type(self).__name__
        self._cfg            = cfg
        self._session        = session
        self._log            = self._session._log
        self._prof           = self._session._prof
        self.requested_cores = self._cfg['cores']

        self._log.info("Configuring ResourceManager %s.", self.name)

        self.lm_info            = dict()
        self.rm_info            = dict()
        self.slot_list          = list()
        self.node_list          = list()
        self.partitions         = dict()
        self.agent_nodes        = dict()
        self.sockets_per_node   = None
        self.cores_per_socket   = None
        self.gpus_per_socket    = None
        self.lfs_per_node       = None
        self.mem_per_node       = None
        self.smt                = int(os.environ.get('RADICAL_SAGA_SMT', 1))

        # The ResourceManager will possibly need to reserve nodes for the agent, according
        # to the agent layout.  We dig out the respective requirements from the
        # config right here.
        self._agent_reqs = []
        agents = self._cfg.get('agents', {})

        # FIXME: this loop iterates over all agents *defined* in the layout, not
        #        over all agents which are to be actually executed, thus
        #        potentially reserving too many nodes.a
        # NOTE:  this code path is *within* the agent, so at least agent.0
        #        cannot possibly land on a different node.
        for agent in agents:
            target = agents[agent].get('target')
            # make sure that the target either 'local', which we will ignore,
            # or 'node'.
            if target == 'local':
                pass  # ignore that one
            elif target == 'node':
                self._agent_reqs.append(agent)
            else :
                raise ValueError("ill-formatted agent target '%s'" % target)

        # We are good to get rolling, and to detect the runtime environment of
        # the local ResourceManager.
        self._configure()
        self._log.info("Discovered execution environment: %s", self.node_list)

        # Make sure we got a valid nodelist and a valid setting for
        # cores_per_socket and sockets_per_node
        if not self.node_list        or\
           self.sockets_per_node < 1 or \
           self.cores_per_socket < 1:
            raise RuntimeError('ResourceManager configuration invalid (%s)(%s)(%s)' %
                    (self.node_list, self.sockets_per_node,
                     self.cores_per_socket))

        # Check if the ResourceManager implementation reserved agent nodes.  If not, pick
        # the first couple of nodes from the nodelist as a fallback.
        if self._agent_reqs and not self.agent_nodes:
            self._log.info('Determine list of agent nodes generically.')
            for agent in self._agent_reqs:
                # Get a node from the end of the node list
                self.agent_nodes[agent] = self.node_list.pop()
                # If all nodes are taken by workers now, we can safely stop,
                # and let the raise below do its thing.
                if not self.node_list:
                    break

        if self.agent_nodes:
            self._log.info('agents      : %s' % list(self.agent_nodes.keys()))
            self._log.info('agent nodes : %s' % list(self.agent_nodes.values()))
            self._log.info('worker nodes: %s' % self.node_list)

        # Check if we can do any work
        if not self.node_list:
            raise RuntimeError('ResourceManager has no nodes left to run tasks')

        # After ResourceManager configuration, we call any existing config hooks
        # on the launch methods.  Those hooks may need to adjust the
        # ResourceManager settings.  We only call LaunchMethod hooks *once*
        launch_methods = set()  # set keeps entries unique
        if 'mpi_launch_method' in self._cfg:
            launch_methods.add(self._cfg['mpi_launch_method'])
        launch_methods.add(self._cfg['task_launch_method'])
        launch_methods.add(self._cfg['agent_launch_method'])

        for lm in launch_methods:
            try:
                ru.dict_merge(
                    self.lm_info,
                    rpa.LaunchMethod.rm_config_hook(name=lm,
                                                    cfg=self._cfg,
                                                    rm=self,
                                                    log=self._log,
                                                    profiler=self._prof))
            except:
                self._log.exception("ResourceManager config hook failed")
                raise

            self._log.info("ResourceManager config hook succeeded (%s)" % lm)

        # check for partitions after `rm_config_hook` was applied
        # NOTE: partition ids should be of a string type, because of JSON
        #       serialization (agent config), which keeps dict keys as strings
        if self.lm_info.get('partitions'):
            for k, v in self.lm_info['partitions'].items():
                self.partitions[str(k)] = v['nodes']
                del v['nodes']  # do not keep nodes in `lm_info['partitions']`

      # # For now assume that all nodes have equal amount of cores and gpus
      # cores_avail = (len(self.node_list) + len(self.agent_nodes)) \
      #             * self.cores_per_socket * self.sockets_per_node
      # gpus_avail  = (len(self.node_list) + len(self.agent_nodes)) \
      #             * self.gpus_per_socket * self.sockets_per_node


        # NOTE: self.rm_info is what scheduler and launch method can
        #       ultimately use, as it is included into the cfg passed to all
        #       components.
        #
        # it defines
        #   lm_info:            dict received via the LM's rm_config_hook
        #   node_list:          list of node names to be used for task execution
        #   partitions:         a dict with partition id and list of node uids
        #   sockets_per_node:   integer number of sockets on a node
        #   cores_per_socket:   integer number of cores per socket
        #   gpus_per_socket:    integer number of gpus per socket
        #   agent_nodes:        list of node names reserved for agent execution
        #   lfs_per_node:       dict consisting the path and size of lfs on each node
        #   mem_per_node:       number of MB per node
        #   smt:                threads per core (exposed as core in RP)
        #

        self.rm_info = {
            'name'             : self.name,
            'lm_info'          : self.lm_info,
            'node_list'        : self.node_list,
            'partitions'       : self.partitions,
            'sockets_per_node' : self.sockets_per_node,
            'cores_per_socket' : self.cores_per_socket * self.smt,
            'gpus_per_socket'  : self.gpus_per_socket,
            'cores_per_node'   : self.sockets_per_node * self.cores_per_socket * self.smt,
            'gpus_per_node'    : self.sockets_per_node * self.gpus_per_socket,
            'agent_nodes'      : self.agent_nodes,
            'lfs_per_node'     : self.lfs_per_node,
            'mem_per_node'     : self.mem_per_node,
            'smt'              : self.smt
        }


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        lsf_hostfile = os.environ.get('LSB_DJOB_HOSTFILE')
        if not lsf_hostfile:
            raise RuntimeError("$LSB_DJOB_HOSTFILE not set!")
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

        self._log.debug('found %d nodes: %s', len(lsf_nodes), lsf_nodes)

        # RP currently requires uniform node configuration, so we expect the
        # same core count for all nodes
        assert(len(set(lsf_nodes.values())) == 1)
        lsf_cores_per_node = list(lsf_nodes.values())[0]
        self._log.debug('found %d nodes with %d cores', len(lsf_nodes),
                                                        lsf_cores_per_node)

        # We cannot inspect gpu and socket numbers yet (TODO), so pull those
        # from the configuration
        lsf_sockets_per_node = self._cfg.get('sockets_per_node', 1)
        lsf_gpus_per_node    = self._cfg.get('gpus_per_node',    0)
        lsf_mem_per_node     = self._cfg.get('mem_per_node',     0)

        # ensure we can derive the number of cores per socket
        assert(not lsf_cores_per_node % lsf_sockets_per_node)
        lsf_cores_per_socket = int(lsf_cores_per_node / lsf_sockets_per_node)

        # same for gpus
        assert(not lsf_gpus_per_node % lsf_sockets_per_node)
        lsf_gpus_per_socket = int(lsf_gpus_per_node / lsf_sockets_per_node)

        # get lfs info from configs, too
        lsf_lfs_per_node = {'path': self._cfg.get('lfs_path_per_node', None),
                            'size': self._cfg.get('lfs_size_per_node', 0)}

        # structure of the node list is
        #
        #   [[node_name_1, node_uid_1],
        #    [node_name_2, node_uid_2],
        #    ...
        #   ]
        #
        # While LSF node names are unique and could serve as node uids, we
        # need an integer index later on for resource set specifications.
        # (LSF starts node indexes at 1, not 0)

        node_names     = sorted(lsf_nodes.keys())
        self.node_list = [[n, str(i + 1)] for i, n
                                          in  enumerate(node_names)]
        self._log.debug('node list: %s', pprint.pformat(self.node_list))

        self.sockets_per_node = lsf_sockets_per_node
        self.cores_per_socket = lsf_cores_per_socket
        self.gpus_per_socket  = lsf_gpus_per_socket
        self.cores_per_node   = lsf_cores_per_socket * lsf_sockets_per_node
        self.gpus_per_node    = lsf_gpus_per_socket * lsf_sockets_per_node
        self.lfs_per_node     = lsf_lfs_per_node
        self.mem_per_node     = lsf_mem_per_node


# ------------------------------------------------------------------------------

