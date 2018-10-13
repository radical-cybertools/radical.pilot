
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__ = "MIT"


import os

from base import LRMS


# ==============================================================================
#
class LSF_SUMMIT(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        # We call the base class constructor to make sure we don't 
        # miss any future critical changes. But as such, 
        # the LSF_SUMMIT LRMS, currently, does not need the base
        # constructor due to the difference in the assumed node
        # structure. If the node-structure with sockets needs to
        # be extended to other LRMS we will change the base 
        # constructor.
        LRMS.__init__(self, cfg, session)

        self.name            = type(self).__name__
        self._cfg            = cfg
        self._session        = session
        self._log            = self._session._log
        self._prof           = self._session._prof
        self.requested_cores = self._cfg['cores']

        self._log.info("Configuring LRMS %s.", self.name)

        self.lm_info            = dict()
        self.lrms_info          = dict()
        self.slot_list          = list()
        self.node_list          = list()
        self.agent_nodes        = dict()
        self.sockets_per_node   = None
        self.cores_per_socket   = None
        self.gpus_per_socket    = None
        self.lfs_per_node       = None

        # The LRMS will possibly need to reserve nodes for the agent, according
        # to the agent layout.  We dig out the respective requirements from the
        # config right here.
        self._agent_reqs = []
        agents = self._cfg.get('agents', {})

        # FIXME: this loop iterates over all agents *defined* in the layout, not
        #        over all agents which are to be actually executed, thus
        #        potentially reserving too many nodes.a
        # NOTE:  this code path is *within* the agent, so at least agent_0
        #        cannot possibly land on a different node.
        for agent in agents:
            target = agents[agent].get('target')
            # make sure that the target either 'local', which we will ignore,
            # or 'node'.
            if target == 'local':
                pass # ignore that one
            elif target == 'node':
                self._agent_reqs.append(agent)
            else :
                raise ValueError("ill-formatted agent target '%s'" % target)

        # We are good to get rolling, and to detect the runtime environment of
        # the local LRMS.
        self._configure()
        self._log.info("Discovered execution environment: %s", self.node_list)

        # Make sure we got a valid nodelist and a valid setting for
        # cores_per_node
        if not self.node_list or self.sockets_per_node < 1 or self.cores_per_socket < 1:
            raise RuntimeError('LRMS configuration invalid (%s)(%s)(%s)' % \
                    (self.node_list, self.sockets_per_node, self.cores_per_socket))

        # Check if the LRMS implementation reserved agent nodes.  If not, pick
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
            self._log.info('Reserved agent node(s): %s' % self.agent_nodes.values())
            self._log.info('Agent(s) running on node(s): %s' % self.agent_nodes.keys())
            self._log.info('Remaining work node(s): %s' % self.node_list)

        # Check if we can do any work
        if not self.node_list:
            raise RuntimeError('LRMS has no nodes left to run units')

        # After LRMS configuration, we call any existing config hooks on the
        # launch methods.  Those hooks may need to adjust the LRMS settings
        # (hello ORTE).  We only call LM hooks *once*
        launch_methods = set() # set keeps entries unique
        if 'mpi_launch_method' in self._cfg:
            launch_methods.add(self._cfg['mpi_launch_method'])
        launch_methods.add(self._cfg['task_launch_method'])
        launch_methods.add(self._cfg['agent_launch_method'])

        for lm in launch_methods:
            if lm:
                try:
                    from .... import pilot as rp
                    ru.dict_merge(self.lm_info,
                            rp.agent.LM.lrms_config_hook(lm, self._cfg, self,
                                self._log, self._prof))
                except Exception as e:
                    self._log.exception("lrms config hook failed")
                    raise

                self._log.info("lrms config hook succeeded (%s)" % lm)

        # For now assume that all nodes have equal amount of cores and gpus
        cores_avail = (len(self.node_list) + len(self.agent_nodes)) * self.cores_per_socket * self.sockets_per_node
        gpus_avail  = (len(self.node_list) + len(self.agent_nodes)) * self.gpus_per_socket * self.sockets_per_node
        if 'RADICAL_PILOT_PROFILE' not in os.environ:
            if cores_avail < int(self.requested_cores):
                raise ValueError("Not enough cores available (%s) to satisfy allocation request (%s)." \
                                % (str(cores_avail), str(self.requested_cores)))

        # NOTE: self.lrms_info is what scheduler and launch method can
        # ultimately use, as it is included into the cfg passed to all
        # components.
        #
        # five elements are well defined:
        #   lm_info:        the dict received via the LM's lrms_config_hook
        #   node_list:      a list of node names to be used for unit execution
        #   cores_per_node: as the name says
        #   gpus_per_node:  as the name says
        #   agent_nodes:    list of node names reserved for agent execution
        #
        # That list may turn out to be insufficient for some schedulers.  Yarn
        # for example may need to communicate YARN service endpoints etc.  an
        # LRMS can thus expand this dict, but is then likely bound to a specific
        # scheduler which can interpret the additional information.
        self.lrms_info['name']              = self.name
        self.lrms_info['lm_info']           = self.lm_info
        self.lrms_info['node_list']         = self.node_list
        self.lrms_info['sockets_per_node']  = self.sockets_per_node
        self.lrms_info['cores_per_socket']  = self.cores_per_socket
        self.lrms_info['gpus_per_socket']   = self.gpus_per_socket
        self.lrms_info['agent_nodes']       = self.agent_nodes
        self.lrms_info['lfs_per_node']      = self.lfs_per_node

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
        lsf_sockets_per_node = self._cfg.get('sockets_per_node', 1)

        # For now, assume all sockets have an equal number of cores and gpus
        lsf_cores_per_socket = min(lsf_core_counts) / lsf_sockets_per_node
        lsf_gpus_per_socket  = self._cfg.get('gpus_per_node', 0) \
                                                    / lsf_sockets_per_node

        lsf_lfs_per_node = {'path': self._cfg.get('lfs_path_per_node', None),
                            'size': self._cfg.get('lfs_size_per_node', 0)}

        self._log.info("Found unique core counts: %s Using: %d",
                       lsf_core_counts, lsf_cores_per_socket)

        # node names are unique, so can serve as node uids
        self.node_list        = [[node, node] for node in lsf_node_list]
        self.sockets_per_node = lsf_sockets_per_node
        self.cores_per_socket = lsf_cores_per_socket
        self.gpus_per_socket  = lsf_gpus_per_socket
        self.lfs_per_node     = lsf_lfs_per_node


# ------------------------------------------------------------------------------

