
__copyright__ = 'Copyright 2016, http://radical.rutgers.edu'
__license__   = 'MIT'

import os

import radical.utils as ru

from ... import agent as rpa


# 'enum' for resource manager types
RM_NAME_FORK        = 'FORK'
RM_NAME_CCM         = 'CCM'
RM_NAME_LOADLEVELER = 'LOADLEVELER'
RM_NAME_LSF         = 'LSF'
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
    '''
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

    Last but not least, the RM will intialize the launch methods and ensure that
    the executor (or any other component really) finds them ready to use.
    '''

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

        self._log.info('Configuring ResourceManager %s.', self.name)

        self.lm_info         = dict()
        self.rm_info         = dict()
        self.node_list       = list()
        self.agent_nodes     = dict()
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
            else:
                raise ValueError('ill-formatted agent target %s' % target)

        # We are good to get rolling, and to detect the runtime environment of
        # the local ResourceManager.
        self._configure()
        self._log.info('Discovered execution environment: %s', self.node_list)

        # Make sure we got a valid nodelist and a valid setting for
        # cores_per_node
        if not self.node_list or self.cores_per_node < 1:
            raise RuntimeError('ResourceManager configuration invalid (%s)(%s)' %
                    (self.node_list, self.cores_per_node))

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

        if self.agent_nodes:
            self._log.info('Reserved nodes: %s' % list(self.agent_nodes.values()))
            self._log.info('Agent    nodes: %s' % list(self.agent_nodes.keys()))
            self._log.info('Worker   nodes: %s' % self.node_list)

        # Check if we can do any work
        if not self.node_list:
            raise RuntimeError('ResourceManager has no nodes left to run tasks')

        # After ResourceManager configuration, we will instantiate all launch
        # methods and call their `configure` method to set them up.  Any other
        # component can then instantiate the LM's without calling `configure`
        # again.
        #
        # Note that the LM configure may need to adjust the ResourceManager
        # settings (hello ORTE).
        launch_methods       = self._cfg.resource_cfg.launch_methods
        self._launchers      = dict()

        for name, lmcfg in launch_methods.items():
            if name == 'order':
                self._launch_order = lmcfg
                continue

            try:
                lm = rpa.LaunchMethod.create(name, self._cfg,
                                                   self._log, self._prof)

                self.lm_info[name]    = lm.initialize(self, lmcfg)
                self._launchers[name] = lm

            except:
                self._log.exception('skip LM %s' % name)

        if not self._launchers:
            raise RuntimeError('no valid launch methods found')

        # For now assume that all nodes have equal amount of cores and gpus
        cores_avail = (len(self.node_list) + len(self.agent_nodes)) * self.cores_per_node
        gpus_avail  = (len(self.node_list) + len(self.agent_nodes)) * self.gpus_per_node

        # on debug runs, we allow more cpus/gpus to appear than physically exist
        if 'RADICAL_DEBUG' not in os.environ:
            if cores_avail < int(self.requested_cores):
                raise ValueError('Not enough cores available (%s < %s).'
                                % (str(cores_avail), str(self.requested_cores)))

            if gpus_avail < int(self.requested_gpus):
                raise ValueError('Not enough gpus available (%s < %s).'
                                % (str(gpus_avail), str(self.requested_gpus)))


        # NOTE: self.rm_info is what scheduler and launch method can
        # ultimately use, as it is included into the cfg passed to all
        # components.
        #
        # five elements are well defined:
        #   lm_info:        the dict received via the LM's rm_config_hook
        #   node_list:      a list of node names to be used for task execution
        #   cores_per_node: as the name says
        #   gpus_per_node:  as the name says
        #   agent_nodes:    list of node names reserved for agent execution
        #
        # That list may turn out to be insufficient for some schedulers.  Yarn
        # for example may need to communicate YARN service endpoints etc.  an
        # ResourceManager can thus expand this dict, but is then likely bound to
        # a specific scheduler which can interpret the additional information.
        self.rm_info['name']           = self.name
        self.rm_info['node_list']      = self.node_list
        self.rm_info['cores_per_node'] = self.cores_per_node
        self.rm_info['gpus_per_node']  = self.gpus_per_node
        self.rm_info['agent_nodes']    = self.agent_nodes
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
            raise TypeError('ResourceManager Factory only available to base class!')

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
                RM_NAME_COBALT      : Cobalt,
                RM_NAME_YARN        : Yarn,
                RM_NAME_SPARK       : Spark,
                RM_NAME_DEBUG       : Debug
            }[name]
            return impl(cfg, session)

        except KeyError as e:
            raise RuntimeError('ResourceManager %s unknown' % name) from e


    # --------------------------------------------------------------------------
    #
    def stop(self):

        # clean up launch methods
        for name in self._launchers:
            try:
                self._launchers[name].finalize()

            except Exception as e:
                # this is not fatal
                self._log.exception('LM %s finalize failed' % name)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        raise NotImplementedError('_Configure missing for %s' % self.name)


    # --------------------------------------------------------------------------
    #
    def find_launcher(self, task):

        # NOTE: this code is duplicated from the executor base class

        for name in self._launch_order:

            launcher = self._launchers[name]
            if launcher.can_launch(task):
                return launcher

        return None


# ------------------------------------------------------------------------------

