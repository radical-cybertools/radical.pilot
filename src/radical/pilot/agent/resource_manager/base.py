
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os

import radical.utils as ru

from ... import agent as rpa


# 'enum' for resource manager types
RM_NAME_FORK        = 'FORK'
RM_NAME_CCM         = 'CCM'
RM_NAME_LOADLEVELER = 'LOADLEVELER'
RM_NAME_LSF         = 'LSF'
RM_NAME_LSF_SUMMIT  = 'LSF_SUMMIT'
RM_NAME_PBSPRO      = 'PBSPRO'
RM_NAME_SGE         = 'SGE'
RM_NAME_SLURM       = 'SLURM'
RM_NAME_TORQUE      = 'TORQUE'
RM_NAME_COBALT      = 'COBALT'
RM_NAME_YARN        = 'YARN'
RM_NAME_SPARK       = 'SPARK'
RM_NAME_DEBUG       = 'DEBUG'


# ------------------------------------------------------------------------------
#
# Base class for ResourceManager implementations.
#
class ResourceManager(object):
    """
    The Resource Manager provides three fundamental information:

      ResourceManager.node_list      : list of node names
      ResourceManager.agent_node_list: list of nodes reserved for agent procs
      ResourceManager.cores_per_node : number of cores each node has available
      ResourceManager.gpus_per_node  : number of gpus  each node has available

    Schedulers can rely on these information to be available.  Specific
    ResourceManager incarnation may have additional information available -- but
    schedulers relying on those are invariably bound to the specific
    ResourceManager.  An example is the Torus Scheduler which relies on detailed
    torus layout information from the LoadLevelerRM (which describes the BG/Q).

    The ResourceManager will reserve nodes for the agent execution, by deriving
    the respectively required node count from the config's 'agents' section.
    Those nodes will be listed in ResourceManager.agent_node_list. Schedulers
    MUST NOT use the agent_node_list to place tasks -- CUs are limited
    to the nodes in ResourceManager.node_list.
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
        self.requested_gpus  = self._cfg['gpus']

        self._log.info("Configuring ResourceManager %s.", self.name)

        self.lm_info         = dict()
        self.rm_info         = dict()
        self.node_list       = list()
        self.partitions      = dict()
        self.agent_nodes     = dict()
        self.service_node    = None
        self.cores_per_node  = 0
        self.gpus_per_node   = 0
        self.lfs_per_node    = 0
        self.mem_per_node    = 0
        self.smt             = int(os.environ.get('RADICAL_SAGA_SMT', 1))

        # The ResourceManager will possibly need to reserve nodes for the agent,
        # according to the agent layout.  We dig out the respective requirements
        # from the config right here.
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
        # cores_per_node
        if not self.node_list or self.cores_per_node < 1:
            raise RuntimeError('ResourceManager configuration invalid (%s)(%s)' %
                    (self.node_list, self.cores_per_node))

        # reserve a service node if needed
        if os.path.isfile('./services'):
            self.service_node = self.node_list.pop()
            self._log.debug('service node: %s', self.service_node)

        # Check if the ResourceManager implementation reserved agent nodes.
        # If not, pick the first couple of nodes from the nodelist as fallback.
        if self._agent_reqs and not self.agent_nodes:
            self._log.info('Determine list of agent nodes generically.')
            for agent in self._agent_reqs:
                # Get a node from the end of the node list
                self.agent_nodes[agent] = self.node_list.pop()
                # If all nodes are taken by workers now, we can safely stop,
                # and let the raise below do its thing.
                if not self.node_list:
                    break

            self._log.info('Reserved nodes: %s' % list(self.agent_nodes.values()))
            self._log.info('Agent    nodes: %s' % list(self.agent_nodes.keys()))
            self._log.info('Worker   nodes: %s' % self.node_list)

        # Check if we can do any work
        if not self.node_list:
            raise RuntimeError('ResourceManager has no nodes left to run tasks')

        # After ResourceManager configuration, we call any existing config hooks
        # on the launch methods.  Those hooks may need to adjust the
        # ResourceManager settings (hello ORTE).  We only call LaunchMethod
        # hooks *once* (thus the set)
        launch_methods = set()
        launch_methods.add(self._cfg.get('mpi_launch_method'))
        launch_methods.add(self._cfg.get('task_launch_method'))
        launch_methods.add(self._cfg.get('agent_launch_method'))

        launch_methods.discard(None)

        for lm in launch_methods:
            try:
                ru.dict_merge(
                    self.lm_info,
                    rpa.LaunchMethod.rm_config_hook(name=lm,
                                                    cfg=self._cfg,
                                                    rm=self,
                                                    log=self._log,
                                                    profiler=self._prof))
            except Exception as e:
                # FIXME don't catch/raise
                self._log.exception(
                    "ResourceManager config hook failed: %s" % e)
                raise

            self._log.info("ResourceManager config hook succeeded (%s)" % lm)

        # check for partitions after `rm_config_hook` was applied
        # NOTE: partition ids should be of a string type, because of JSON
        #       serialization (agent config), which keeps dict keys as strings
        if self.lm_info.get('partitions'):
            for k, v in self.lm_info['partitions'].items():
                self.partitions[str(k)] = v['nodes']
                del v['nodes']  # do not keep nodes in `lm_info['partitions']`

        # For now assume that all nodes have equal amount of cores and gpus
        cores_avail = (len(self.node_list) + len(self.agent_nodes)) * self.cores_per_node
        gpus_avail  = (len(self.node_list) + len(self.agent_nodes)) * self.gpus_per_node

        # on debug runs, we allow more cpus/gpus to appear than physically exist
        if 'RADICAL_DEBUG' not in os.environ:
            if cores_avail < int(self.requested_cores):
                raise ValueError("Not enough cores available (%s < %s)."
                                % (str(cores_avail), str(self.requested_cores)))

            if gpus_avail < int(self.requested_gpus):
                raise ValueError("Not enough gpus available (%s < %s)."
                                % (str(gpus_avail), str(self.requested_gpus)))


        # NOTE: self.rm_info is what scheduler and launch method can
        # ultimately use, as it is included into the cfg passed to all
        # components.
        #
        # five elements are well defined:
        #   lm_info:        the dict received via the LM's rm_config_hook
        #   node_list:      a list of node names to be used for task execution
        #   partitions:     a dict with partition id and list of node uids
        #   cores_per_node: as the name says
        #   gpus_per_node:  as the name says
        #   agent_nodes:    list of node names reserved for agent execution
        #
        # That list may turn out to be insufficient for some schedulers.  Yarn
        # for example may need to communicate YARN service endpoints etc.  an
        # ResourceManager can thus expand this dict, but is then likely bound to
        # a specific scheduler which can interpret the additional information.
        self.rm_info['name']           = self.name
        self.rm_info['lm_info']        = self.lm_info
        self.rm_info['node_list']      = self.node_list
        self.rm_info['partitions']     = self.partitions
        self.rm_info['cores_per_node'] = self.cores_per_node
        self.rm_info['gpus_per_node']  = self.gpus_per_node
        self.rm_info['agent_nodes']    = self.agent_nodes
        self.rm_info['service_node']   = self.service_node
        self.rm_info['lfs_per_node']   = self.lfs_per_node
        self.rm_info['mem_per_node']   = self.mem_per_node


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the
    # ResourceManager.
    #
    @classmethod
    def create(cls, name, cfg, session):

        from .ccm         import CCM
        from .fork        import Fork
        from .loadleveler import LoadLeveler
        from .lsf         import LSF
        from .lsf_summit  import LSF_SUMMIT
        from .pbspro      import PBSPro
        from .sge         import SGE
        from .slurm       import Slurm
        from .torque      import Torque
        from .cobalt      import Cobalt
        from .yarn        import Yarn
        from .spark       import Spark
        from .debug       import Debug

        # Make sure that we are the base-class!
        if cls != ResourceManager:
            raise TypeError("ResourceManager Factory only available to base class!")

        try:
            impl = {
                RM_NAME_FORK        : Fork,
                RM_NAME_CCM         : CCM,
                RM_NAME_LOADLEVELER : LoadLeveler,
                RM_NAME_LSF         : LSF,
                RM_NAME_LSF_SUMMIT  : LSF_SUMMIT,
                RM_NAME_PBSPRO      : PBSPro,
                RM_NAME_SGE         : SGE,
                RM_NAME_SLURM       : Slurm,
                RM_NAME_TORQUE      : Torque,
                RM_NAME_COBALT      : Cobalt,
                RM_NAME_YARN        : Yarn,
                RM_NAME_SPARK       : Spark,
                RM_NAME_DEBUG       : Debug
            }[name]
            return impl(cfg, session)

        except KeyError as e:
            raise RuntimeError("ResourceManager '%s' unknown" % name) from e


    # --------------------------------------------------------------------------
    #
    def stop(self):

        # During ResourceManager termination, we call any existing shutdown hooks on the
        # launch methods.  We only call LaunchMethod shutdown hooks *once*
        launch_methods = set()
        launch_methods.add(self._cfg.get('mpi_launch_method'))
        launch_methods.add(self._cfg.get('task_launch_method'))
        launch_methods.add(self._cfg.get('agent_launch_method'))

        launch_methods.discard(None)

        for lm in launch_methods:
            try:
                ru.dict_merge(
                    self.lm_info,
                    rpa.LaunchMethod.rm_shutdown_hook(name=lm,
                                                      cfg=self._cfg,
                                                      rm=self,
                                                      lm_info=self.lm_info,
                                                      log=self._log,
                                                      profiler=self._prof))
            except Exception as e:
                self._log.exception(
                    "ResourceManager shutdown hook failed: %s" % e)
                raise

            self._log.info("ResourceManager shutdown hook succeeded (%s)" % lm)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        raise NotImplementedError("_Configure missing for %s" % self.name)


# ------------------------------------------------------------------------------

