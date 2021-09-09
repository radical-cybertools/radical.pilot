
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import math
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


class RMInfo(ru.Munch):
    '''
    Each resource manager instance must gather provide the information defined
    in this class.  Additional attributes can be attached, but should then only
    be evaluated by launch methods which are tightly bound to the resource
    manager type ('friends' in C++ speak).
    '''

    _schema = {
            'requested_nodes'  : int,           # number of requested nodes
            'requested_cores'  : int,           # number of requested cores
            'requested_gpus'   : int,           # number of requested gpus

            'partitions'       : {int: None},   # partition setup
            'node_list'        : [None],        # tuples of node uids and names
            'agent_node_list'  : [None],        # nodes reserved for sub-agents
            'service_node_list': [None],        # nodes reserved for services

            'sockets_per_node' : int,           # number of sockets per node
            'cores_per_socket' : int,           # number of cores per socket
            'threads_per_core' : int,           # number of threads per core

            'gpus_per_socket'  : int,           # number of gpus per socket
            'threads_per_gpu'  : int,           # number of threads per cgpu
            'mem_per_gpu'      : int,           # memory per gpu (MB)

            'lfs_per_node'     : int,           # node local FS size (MB)
            'lfs_path'         : str,           # node local FS path
            'mem_per_node'     : int,           # memory per node (MB)

            'details'          : {None: None},  # dict of launch method info
            'lm_info'          : {str: None},   # dict of launch method info

            # backward compatibility / simplicity
            'cores_per_node'   : int,
            'gpus_per_node'    : int,
            'threads_per_node' : int,
    }

    _defaults = {
            'agent_node_list'  : [],            # no sub-agents run by default
            'service_node_list': [],            # no services run by default
            'threads_per_core' : 1,             # assume one thread per core
            'threads_per_gpu'  : 1,             # assume one thread per gpu
    }


    # --------------------------------------------------------------------------
    #
    def _verify(self):

        if not self.requested_nodes:
            if self.requested_cores and self.cores_per_node:
                self.requested_cores = self.requested_cores / self.cores_per_node

        if not self.requested_cores:
            if self.requested_nodes and self.cores_per_node:
                self.requested_cores = self.requested_nodes * self.cores_per_node

        if not self.requested_gpus:
            if self.requested_nodes and self.gpus_per_node:
                self.requested_gpus = self.requested_nodes * self.gpus_per_node

        assert(self['requested_nodes'  ] is not None)
        assert(self['requested_cores'  ] is not None)
        assert(self['requested_gpus'   ] is not None)

      # assert(self['partitions'       ] is not None)
        assert(self['node_list'        ] is not None)
        assert(self['agent_node_list'  ] is not None)
        assert(self['service_node_list'] is not None)

        assert(self['cores_per_node'   ] is not None)
        assert(self['gpus_per_node'    ] is not None)
        assert(self['threads_per_core' ] is not None)


# ------------------------------------------------------------------------------
#
# Base class for ResourceManager implementations.
#
class ResourceManager(object):
    '''
    The Resource Manager provides fundamental resource information via
    `self.info` (see `RMInfo` class definition).

      ResourceManager.node_list      : list of nodes names and uids
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
    MUST NOT use the agent_node_list to place tasks -- Tasks are limited
    to the nodes in ResourceManager.node_list.

    Last but not least, the RM will initialize launch methods and ensure that
    the executor (or any other component really) finds them ready to use.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, log, prof):

        self.name     = type(self).__name__
        self._cfg     = cfg
        self._log     = log
        self._prof    = prof

        self._log.info('Configuring ResourceManager %s', self.name)

        self._reg = ru.zmq.RegistryClient(url=self._cfg.reg_addr)
        rm_info   = self._reg.get('rm.%s' % self.name.lower())

        if rm_info:

            self._log.debug('=== RM init from registry')
            rm_info = RMInfo(rm_info)

        else:
            self._log.debug('=== RM init from scratch')

            # let the base class collect some data, then let the impl take over
            rm_info = self._prepare_info()
            rm_info = self._init_from_scratch(rm_info)

            # after rm setup and node config, set up all launch methods
            self._prepare_launch_methods(rm_info)

            # have a valid info - store in registry and complete initialization
            self._reg.put('rm.%s' % self.name.lower(), rm_info)

        self._reg.close()
        self._set_info(rm_info)


    # --------------------------------------------------------------------------
    #
    @property
    def info(self):

        return self._rm_info


    # --------------------------------------------------------------------------
    #
    def _set_info(self, info):

        self._rm_info = info
        self._rm_info.verify()


    # --------------------------------------------------------------------------
    #
    def _prepare_info(self):

        info = RMInfo()

        # fill well defined attributes
        info.requested_cores  = self._cfg['cores']
        info.requested_gpus   = self._cfg['gpus']

        rcfg = self._cfg['resource_cfg']
        info.cores_per_node   = rcfg.get('cores_per_node')
        info.gpus_per_node    = rcfg.get('gpus_per_node')
        info.mem_per_node     = rcfg.get('mem_per_node',      0)
        info.lfs_per_node     = rcfg.get('lfs_size_per_node', 0)
        info.lfs_path         = ru.expand_env(rcfg.get('lfs_path_per_node'))

        info.sockets_per_node = rcfg.get('sockets_per_node',  1)
        info.cores_per_socket = rcfg.get('cores_per_socket')
        info.gpus_per_socket  = rcfg.get('gpus_per_socket')

        if info.cores_per_socket and not info.cores_per_node:
            info.cores_per_node    = info.cores_per_socket * \
                                     info.sockets_per_node
            if info.gpus_per_socket:
                info.gpus_per_node = info.gpus_per_socket * \
                                     info.sockets_per_node
        elif info.cores_per_node:
            info.sockets_per_node  = 1
            info.cores_per_socket  = info.cores_per_node
            info.gpus_per_socket   = info.gpus_per_node

        info.threads_per_core = int(os.environ.get('RADICAL_SAGA_SMT', 1))
        info.threads_per_gpu  = 1
        info.mem_per_gpu      = None

        n_nodes = info.requested_cores / info.cores_per_node
        if info.gpus_per_node:
            n_nodes = max(n_nodes, info.requested_gpus / info.gpus_per_node)

        info.requested_nodes = int(math.ceil(n_nodes))

        return info


    # --------------------------------------------------------------------------
    #
    def _update_info(self, info):

        return info


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, info):

        # let the specific RM instance fill out the RMInfo attributes
        info = self._update_info(info)

        # we expect to have a valid node list now
        assert info.node_list
        self._log.info('node list: %s', info.node_list)

        # The ResourceManager may need to reserve nodes for sub agents and
        # service, according to the agent layout and pilot config.  We dig out
        # the respective requirements from the node list and complain on
        # insufficient resources
        agent_nodes   = 0
        service_nodes = 0

        for agent in self._cfg.get('agents', {}):
            if self._cfg['agents'][agent].get('target') == 'node':
                agent_nodes += 1

        if os.path.isfile('./services'):
            service_nodes += 1

        # Check if the ResourceManager implementation reserved agent nodes.
        # If not, pick the first couple of nodes from the nodelist as fallback.
        if agent_nodes:

            if not info.agent_node_list:
                info.agent_node_list = list()
                for i in range(agent_nodes):
                    info.agent_node_list.append(info.node_list.pop())

            assert(agent_nodes == len(info.agent_node_list))

        if service_nodes:

            if not info.service_node_list:
                info.service_node_list = list()
                for i in range(service_nodes):
                    info.service_node_list.append(info.node_list.pop())

            assert(service_nodes == len(info.service_node_list))

        self._log.info('compute nodes: %s' % len(info.node_list))
        self._log.info('agent   nodes: %s' % len(info.agent_node_list))
        self._log.info('service nodes: %s' % len(info.service_node_list))

        # check if we can do any work
        if not info.node_list:
            raise RuntimeError('ResourceManager has no nodes left to run tasks')

        # we have nodes and node properties - calculate some convenience values
        # and perform sanity checks
        total_nodes = len(info.node_list) + agent_nodes + service_nodes
        cores_avail = total_nodes * info.cores_per_node
        gpus_avail  = total_nodes * info.gpus_per_node

        assert(total_nodes >= info.requested_nodes)
        assert(cores_avail >= info.requested_cores)
        assert(gpus_avail  >= info.requested_gpus)

        return info


    # --------------------------------------------------------------------------
    #
    def _prepare_launch_methods(self, info):

        launch_methods  = self._cfg.resource_cfg.launch_methods
        self._launchers = dict()

        for name, lm_cfg in launch_methods.items():

            if name == 'order':
                self._launch_order = lm_cfg
                continue

            try:
                self._log.debug('==== prepare lm %s', name)
                lm_cfg['pid']         = self._cfg.pid
                lm_cfg['reg_addr']    = self._cfg.reg_addr
                self._launchers[name] = rpa.LaunchMethod.create(
                    name, lm_cfg, info, self._log, self._prof)

            except:
                self._log.exception('skip LM %s' % name)

        if not self._launchers:
            raise RuntimeError('no valid launch methods found')


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the
    # ResourceManager.
    #
    @classmethod
    def create(cls, name, cfg, log, prof):

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
        }

        if name not in impl:
            raise RuntimeError('ResourceManager %s unknown' % name)

        return impl[name](cfg, log, prof)



    # --------------------------------------------------------------------------
    #
    def stop(self):

        # clean up launch methods
        for name in self._launchers:
            try:    self._launchers[name].finalize()
            except: self._log.exception('LM %s finalize failed', name)


    # --------------------------------------------------------------------------
    #
    def find_launcher(self, task):

        for name in self._launch_order:

            launcher = self._launchers[name]
            if launcher.can_launch(task):
                return launcher

        return None


# ------------------------------------------------------------------------------

