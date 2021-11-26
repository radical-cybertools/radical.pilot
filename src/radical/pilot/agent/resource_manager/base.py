
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import math
import os

from typing import Optional, List, Tuple, Dict, Any

T_NODES     = List[Tuple[str, int]]
T_NODE_LIST = List[Dict[str, Any]]


import radical.utils as ru

from ... import agent     as rpa
from ... import constants as rpc


# 'enum' for resource manager types
RM_NAME_FORK        = 'FORK'
RM_NAME_CCM         = 'CCM'
RM_NAME_LSF         = 'LSF'
RM_NAME_PBSPRO      = 'PBSPRO'
RM_NAME_SLURM       = 'SLURM'
RM_NAME_TORQUE      = 'TORQUE'
RM_NAME_COBALT      = 'COBALT'
RM_NAME_YARN        = 'YARN'
RM_NAME_DEBUG       = 'DEBUG'


# ------------------------------------------------------------------------------
#
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

            'cores_per_node'   : int,           # number of cores per node
            'threads_per_core' : int,           # number of threads per core

            'gpus_per_node'    : int,           # number of gpus per node
            'threads_per_gpu'  : int,           # number of threads per gpu
            'mem_per_gpu'      : int,           # memory per gpu (MB)

            'lfs_per_node'     : int,           # node local FS size (MB)
            'lfs_path'         : str,           # node local FS path
            'mem_per_node'     : int,           # memory per node (MB)

            'details'          : {None: None},  # dict of launch method info
            'lm_info'          : {str: None},   # dict of launch method info
    }

    _defaults = {
            'agent_node_list'  : [],            # no sub-agents run by default
            'service_node_list': [],            # no services run by default
            'cores_per_node'   : 1,
            'threads_per_core' : 1,
            'gpus_per_node'    : 0,
            'threads_per_gpu'  : 1,
    }


    # --------------------------------------------------------------------------
    #
    def _verify(self):

        assert(self['requested_nodes'  ])
        assert(self['requested_cores'  ])
        assert(self['requested_gpus'   ] is not None)

      # assert(self['partitions'       ] is not None)
        assert(self['node_list'])
        assert(self['agent_node_list'  ] is not None)
        assert(self['service_node_list'] is not None)

        assert(self['cores_per_node'   ])
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
    ResourceManager.

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

        self.name  = type(self).__name__
        self._cfg  = cfg
        self._log  = log
        self._prof = prof

        self._log.info('Configuring ResourceManager %s', self.name)

        reg     = ru.zmq.RegistryClient(url=self._cfg.reg_addr)
        rm_info = reg.get('rm.%s' % self.name.lower())

        if rm_info:

            self._log.debug('RM init from registry')
            rm_info = RMInfo(rm_info)
            rm_info.verify()

        else:
            self._log.debug('RM init from scratch')

            # let the base class collect some data, then let the impl take over
            rm_info = self.init_from_scratch()
            rm_info.verify()

            # have a valid info - store in registry and complete initialization
            reg.put('rm.%s' % self.name.lower(), rm_info.as_dict())

        # set up launch methods even when initialized from registry info
        self._prepare_launch_methods(rm_info)

        reg.close()
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
    def _init_from_scratch(self, rm_info: RMInfo) -> RMInfo:
        '''
        This method MUST be overloaded by any RM implementation.  It will be
        called during `init_from_scratch` and is expected to check and correct
        or complete node information, such as `cores_per_node`, `gpus_per_node`
        etc., and to provide `rm_info.node_list` of the following form:

            node_list = [
                {
                    'node_name': str                        # node name
                    'node_id'  : str                        # node uid
                    'cores'    : [rpc.FREE, rpc.FREE, ...]  # cores per node
                    'gpus'     : [rpc.FREE, rpc.FREE, ...]  # gpus per node
                    'lfs'      : int                        # lfs per node (MB)
                    'mem'      : int                        # mem per node (MB)
                },
                ...
            ]

        The node entries can be augmented with additional information which may
        be interpreted by the specific agent scheduler instance.
        '''

        raise NotImplementedError('_update_node_info is not implemented')


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self):

        rm_info = RMInfo()

        # fill well defined default attributes
        rm_info.requested_cores  = self._cfg['cores']
        rm_info.requested_gpus   = self._cfg['gpus']

        rcfg = self._cfg['resource_cfg']
        rm_info.cores_per_node   = rcfg.get('cores_per_node',    0)
        rm_info.gpus_per_node    = rcfg.get('gpus_per_node',     0)
        rm_info.mem_per_node     = rcfg.get('mem_per_node',      0)
        rm_info.lfs_per_node     = rcfg.get('lfs_size_per_node', 0)
        rm_info.lfs_path         = ru.expand_env(rcfg.get('lfs_path_per_node'))

        rm_info.threads_per_core = int(os.environ.get('RADICAL_SAGA_SMT', 1))
        rm_info.threads_per_gpu  = 1
        rm_info.mem_per_gpu      = None

        # let the specific RM instance fill out the RMInfo attributes
        rm_info = self._init_from_scratch(rm_info)

        # we expect to have a valid node list now
        self._log.info('node list: %s', rm_info.node_list)

        # complete node information (cores_per_node, gpus_per_node, etc)
        n_nodes = rm_info.requested_cores / rm_info.cores_per_node
        if rm_info.gpus_per_node:
            gpu_nodes = rm_info.requested_gpus / rm_info.gpus_per_node
            n_nodes   = max(n_nodes, gpu_nodes)

        rm_info.requested_nodes = math.ceil(n_nodes)

        if not rm_info.requested_nodes:
            if rm_info.requested_cores and rm_info.cores_per_node:
                rm_info.requested_nodes = rm_info.requested_cores \
                                        / rm_info.cores_per_node

        if not rm_info.requested_cores:
            if rm_info.requested_nodes and rm_info.cores_per_node:
                rm_info.requested_cores = rm_info.requested_nodes \
                                        * rm_info.cores_per_node

        if not rm_info.requested_gpus:
            if rm_info.requested_nodes and rm_info.gpus_per_node:
                rm_info.requested_gpus = rm_info.requested_nodes \
                                       * rm_info.gpus_per_node

        # however, the config can override core and gpu detection,
        # and decide to block some resources
        blocked_cores = self._cfg.resource_cfg.blocked_cores or []
        blocked_gpus  = self._cfg.resource_cfg.blocked_gpus  or []

        self._log.info('blocked cores: %s' % blocked_cores)
        self._log.info('blocked gpus : %s' % blocked_gpus)

        if blocked_cores or blocked_gpus:

            rm_info.cores_per_node -= len(blocked_cores)
            rm_info.gpus_per_node  -= len(blocked_gpus)

            for node in rm_info.node_list:

                for idx in blocked_cores:
                    assert(len(node['cores']) > idx)
                    node['cores'][idx] = rpc.DOWN

                for idx in blocked_gpus:
                    assert(len(node['gpus']) > idx)
                    node['gpus'][idx] = rpc.DOWN

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

            if not rm_info.agent_node_list:
                for _ in range(agent_nodes):
                    rm_info.agent_node_list.append(rm_info.node_list.pop())

            assert(agent_nodes == len(rm_info.agent_node_list))

        if service_nodes:

            if not rm_info.service_node_list:
                for _ in range(service_nodes):
                    rm_info.service_node_list.append(rm_info.node_list.pop())

            assert(service_nodes == len(rm_info.service_node_list))

        self._log.info('compute nodes: %s' % len(rm_info.node_list))
        self._log.info('agent   nodes: %s' % len(rm_info.agent_node_list))
        self._log.info('service nodes: %s' % len(rm_info.service_node_list))

        # check if we can do any work
        if not rm_info.node_list:
            raise RuntimeError('ResourceManager has no nodes left to run tasks')

        # we have nodes and node properties - calculate some convenience values
        # and perform sanity checks
        total_nodes = len(rm_info.node_list) + agent_nodes + service_nodes
        cores_avail = total_nodes * rm_info.cores_per_node
        gpus_avail  = total_nodes * rm_info.gpus_per_node

        assert(total_nodes >= rm_info.requested_nodes)
        assert(cores_avail >= rm_info.requested_cores)
        assert(gpus_avail  >= rm_info.requested_gpus)

        return rm_info


    # --------------------------------------------------------------------------
    #
    def _prepare_launch_methods(self, rm_info):

        launch_methods  = self._cfg.resource_cfg.launch_methods
        self._launchers = dict()

        for name, lm_cfg in launch_methods.items():

            if name == 'order':
                self._launch_order = lm_cfg
                continue

            try:
                self._log.debug('prepare lm %s', name)
                lm_cfg['pid']         = self._cfg.pid
                lm_cfg['reg_addr']    = self._cfg.reg_addr
                self._launchers[name] = rpa.LaunchMethod.create(
                    name, lm_cfg, rm_info, self._log, self._prof)

            except:
                self._log.exception('skip LM %s' % name)
                print('skip', name)

        if not self._launchers:
            raise RuntimeError('no valid launch methods found')


    # --------------------------------------------------------------------------
    #
    def get_partitions(self):

        # TODO: this makes it impossible to have mutiple launchers with a notion
        #       of partitions

        for lname in self._launchers:

            launcher   = self._launchers[lname]
            partitions = launcher.get_partitions()

            if partitions:
                return partitions


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the
    # ResourceManager.
    #
    @classmethod
    def create(cls, name, cfg, log, prof):

        from .ccm         import CCM
        from .fork        import Fork
        from .lsf         import LSF
        from .pbspro      import PBSPro
        from .slurm       import Slurm
        from .torque      import Torque
        from .cobalt      import Cobalt
        from .yarn        import Yarn
        from .debug       import Debug

        # Make sure that we are the base-class!
        if cls != ResourceManager:
            raise TypeError('ResourceManager Factory only available to base class!')

        impl = {
            RM_NAME_FORK        : Fork,
            RM_NAME_CCM         : CCM,
            RM_NAME_LSF         : LSF,
            RM_NAME_PBSPRO      : PBSPro,
            RM_NAME_SLURM       : Slurm,
            RM_NAME_TORQUE      : Torque,
            RM_NAME_COBALT      : Cobalt,
            RM_NAME_YARN        : Yarn,
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

        errors = list()
        for name in self._launch_order:

            launcher = self._launchers[name]
            lm_can_launch, err_message = launcher.can_launch(task)
            if lm_can_launch:
                return launcher
            else:
                errors.append([name, err_message])

        self._log.error('no launch method for task %s:', task['uid'])
        for name, error in errors:
            self._log.debug('    %s: %s', name, error)

        return None


    # --------------------------------------------------------------------------
    #
    def _parse_nodefile(self, fname: str,
                              cpn  : Optional[int] = 0,
                              smt  : Optional[int] = 1) -> T_NODES:
        '''
        parse the given nodefile and return a list of tuples of the form

            [['node_1', 42 * 4],
             ['node_2', 42 * 4],
             ...
            ]

        where the first tuple entry is the name of the node found, and the
        second entry is the number of entries found for this node.  The latter
        number usually corresponds to the number of process slots available on
        that node.

        Some nodefile formats though have one entry per node, not per slot.  In
        those cases we'll use the passed cores per node (`cpn`) to fill the slot
        count for the returned node list (`cpn` will supercede the detected slot
        count).

        An invalid or un-parsable file will result in an empty list being
        returned.
        '''

        if not smt:
            smt = 1

        self._log.info('using nodefile: %s', fname)
        try:
            nodes = dict()
            with ru.ru_open(fname, 'r') as fin:
                for line in fin.readlines():
                    node = line.strip()
                    assert(' ' not in node)
                    if node in nodes: nodes[node] += 1
                    else            : nodes[node]  = 1

            if cpn:
                for node in nodes:
                    nodes[node] = cpn

            # convert node dict into tuple list
            return [(node, cpn * smt) for node, cpn in nodes.items()]

        except Exception:
            return []


    # --------------------------------------------------------------------------
    #
    def _get_cores_per_node(self, nodes: T_NODES) -> Optional[int]:
        '''
        From a node dict as returned by `self._parse_nodefile()`, determine the
        number of cores per node.  To do so, we check if all nodes have the same
        number of cores.  If that is the case we return that number.  If the
        node list is heterogeneous we will raise an `ValueError`.
        '''

        cores_per_node = set([node[1] for node in nodes])

        if len(cores_per_node) == 1:
            cores_per_node = cores_per_node.pop()
            self._log.debug('found %d [%d cores]', len(nodes), cores_per_node)
            return cores_per_node

        else:
            raise ValueError('non-uniform node list, cores_per_node invalid')


    # --------------------------------------------------------------------------
    #
    def _get_node_list(self, nodes  : T_NODES,
                             rm_info: RMInfo) -> T_NODE_LIST:
        '''
        From a node dict as returned by `self._parse_nodefile()`, and from
        additonal per-node information stored in rm_info, create a node list
        as required for rm_info.
        '''

        node_list = [{'node_name': node[0],
                      'node_id'  : node[0],
                      'cores'    : [rpc.FREE] * node[1],
                      'gpus'     : [rpc.FREE] * rm_info.gpus_per_node,
                      'lfs'      : rm_info.lfs_per_node,
                      'mem'      : rm_info.mem_per_node} for node in nodes]

        return node_list


# ------------------------------------------------------------------------------

