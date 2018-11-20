
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.utils as ru


# 'enum' for resource manager types
RM_NAME_FORK        = 'FORK'
RM_NAME_CCM         = 'CCM'
RM_NAME_LOADLEVELER = 'LOADLEVELER'
RM_NAME_LSF         = 'LSF'
RM_NAME_PBSPRO      = 'PBSPRO'
RM_NAME_SGE         = 'SGE'
RM_NAME_SLURM       = 'SLURM'
RM_NAME_TORQUE      = 'TORQUE'
RM_NAME_YARN        = 'YARN'
RM_NAME_SPARK       = 'SPARK'



# ==============================================================================
#
# Base class for LRMS implementations.
#
class LRMS(object):
    """
    The Local Resource Manager (LRMS -- where does the 's' come from, actually?)
    provide three fundamental information:

      LRMS.node_list      : a list of node names
      LRMS.agent_node_list: the list of nodes reserved for agent execution
      LRMS.cores_per_node : the number of cores each node has available
      LRMS.gpus_per_node  : the number of gpus  each node has available

    Schedulers can rely on these information to be available.  Specific LRMS
    incarnation may have additional information available -- but schedulers
    relying on those are invariably bound to the specific LRMS.  An example is
    the Torus Scheduler which relies on detailed torus layout information from
    the LoadLevelerLRMS (which describes the BG/Q).

    The LRMS will reserve nodes for the agent execution, by deriving the
    respectively required node count from the config's 'agents' section.
    Those nodes will be listed in LRMS.agent_node_list. Schedulers MUST NOT use
    the agent_node_list to place compute units -- CUs are limited to the nodes
    in LRMS.node_list.
    """

    # TODO: Core counts dont have to be the same number for all hosts.

    # TODO: We might not have reserved the whole node.

    # TODO: Given that the Agent can determine the real core count, in
    #       principle we could just ignore the config and use as many as we
    #       have to our availability (taken into account that we might not
    #       have the full node reserved of course)
    #       Answer: at least on Yellowstone this doesnt work for MPI,
    #               as you can't spawn more tasks then the number of slots.


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):        

        self.name            = type(self).__name__
        self._cfg            = cfg
        self._session        = session
        self._log            = self._session._log
        self._prof           = self._session._prof
        self.requested_cores = self._cfg['cores']

        self._log.info("Configuring LRMS %s.", self.name)

        self.lm_info         = dict()
        self.lrms_info       = dict()
        self.slot_list       = list()
        self.node_list       = list()
        self.agent_nodes     = dict()
        self.cores_per_node  = None
        self.gpus_per_node   = None
        self.lfs_per_node    = None

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
        if not self.node_list or self.cores_per_node < 1:
            raise RuntimeError('LRMS configuration invalid (%s)(%s)' % \
                    (self.node_list, self.cores_per_node))

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
        cores_avail = (len(self.node_list) + len(self.agent_nodes)) * self.cores_per_node
        gpus_avail  = (len(self.node_list) + len(self.agent_nodes)) * self.gpus_per_node
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
        self.lrms_info['name']           = self.name
        self.lrms_info['lm_info']        = self.lm_info
        self.lrms_info['node_list']      = self.node_list
        self.lrms_info['cores_per_node'] = self.cores_per_node
        self.lrms_info['gpus_per_node']  = self.gpus_per_node
        self.lrms_info['agent_nodes']    = self.agent_nodes
        self.lrms_info['lfs_per_node']   = self.lfs_per_node


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the LRMS.
    #
    @classmethod
    def create(cls, name, cfg, session):

        from .ccm         import CCM        
        from .fork        import Fork       
        from .loadleveler import LoadLeveler
        from .lsf         import LSF        
        from .pbspro      import PBSPro     
        from .sge         import SGE        
        from .slurm       import Slurm      
        from .torque      import Torque     
        from .yarn        import Yarn      
        from .spark       import Spark       

        # Make sure that we are the base-class!
        if cls != LRMS:
            raise TypeError("LRMS Factory only available to base class!")

        try:
            impl = {
                RM_NAME_FORK        : Fork,
                RM_NAME_CCM         : CCM,
                RM_NAME_LOADLEVELER : LoadLeveler,
                RM_NAME_LSF         : LSF,
                RM_NAME_PBSPRO      : PBSPro,
                RM_NAME_SGE         : SGE,
                RM_NAME_SLURM       : Slurm,
                RM_NAME_TORQUE      : Torque,
                RM_NAME_YARN        : Yarn,
                RM_NAME_SPARK       : Spark
            }[name]
            return impl(cfg, session)

        except KeyError:
            session._log.exception('lrms construction error')
            raise RuntimeError("LRMS type '%s' unknown or defunct" % name)


    # --------------------------------------------------------------------------
    #
    def stop(self):

        # During LRMS termination, we call any existing shutdown hooks on the
        # launch methods.  We only call LM shutdown hooks *once*
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
                    rp.agent.LM.lrms_shutdown_hook(lm, self._cfg, self,
                                                    self.lm_info, self._log,
                                                    self._prof))
                except Exception as e:
                    self._log.exception("lrms shutdown hook failed")
                    raise

                self._log.info("lrms shutdown hook succeeded (%s)" % lm)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        raise NotImplementedError("_Configure missing for %s" % self.name)


# ------------------------------------------------------------------------------

