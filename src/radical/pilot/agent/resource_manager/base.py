
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import math
import os

from rc.process import Process
from typing     import Optional, List, Tuple, Dict, Any

T_NODES     = List[Tuple[str, int]]
T_NODE_LIST = List[Dict[str, Any]]


import radical.utils as ru

from ... import agent     as rpa
from ... import utils     as rpu
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
class RMInfo(rpu.FastTypedDict):
    '''
    Each resource manager instance must gather provide the information defined
    in this class.  Additional attributes can be attached, but should then only
    be evaluated by launch methods which are tightly bound to the resource
    manager type ('friends' in C++ speak).
    '''

    _schema = {
            'backup_nodes'         : int,           # number of backup nodes
            'requested_nodes'      : int,           # number of requested nodes
            'requested_cores'      : int,           # number of requested cores
            'requested_gpus'       : int,           # number of requested gpus

            'partition_ids'        : [int],         # partition ids
            'node_list'            : [None],        # tuples of node uids and names
            'backup_list'          : [None],        # list of backup nodes
            'agent_node_list'      : [None],        # nodes reserved for sub-agents
            'service_node_list'    : [None],        # nodes reserved for services

            'cores_per_node'       : int,           # number of cores per node
            'threads_per_core'     : int,           # number of threads per core

            'gpus_per_node'        : int,           # number of gpus per node
            'threads_per_gpu'      : int,           # number of threads per gpu
            'mem_per_gpu'          : int,           # memory per gpu (MB)

            'lfs_per_node'         : int,           # node local FS size (MB)
            'lfs_path'             : str,           # node local FS path
            'mem_per_node'         : int,           # memory per node (MB)

            'details'              : {None: None},  # dict of launch method info
            'launch_methods'       : {str : None},  # dict of launch method cfgs

            'numa_domain_map'      : {int: None},   # resources per numa domain
            'n_partitions'         : int,           # number of partitions
    }

    _defaults = {
            'backup_nodes'         : 0,
            'requested_nodes'      : 0,
            'requested_cores'      : 0,
            'requested_gpus'       : 0,

            'partition_ids'        : list(),
            'node_list'            : list(),
            'backup_list'          : list(),
            'agent_node_list'      : list(),
            'service_node_list'    : list(),

            'cores_per_node'       : 0,
            'threads_per_core'     : 0,

            'gpus_per_node'        : 0,
            'threads_per_gpu'      : 1,
            'mem_per_gpu'          : 0,

            'lfs_per_node'         : 0,
            'lfs_path'             : '/tmp/',
            'mem_per_node'         : 0,

            'details'              : dict(),
            'launch_methods'       : dict(),

            'numa_domain_map'      : dict(),
            'n_partitions'         : 1,
    }


    # --------------------------------------------------------------------------
    #
    def _verify(self):

        assert self['requested_nodes'  ]
        assert self['requested_cores'  ]
        assert self['requested_gpus'   ] is not None

        assert self['node_list'        ]
        assert self['agent_node_list'  ] is not None
        assert self['service_node_list'] is not None

        assert self['cores_per_node'   ]
        assert self['gpus_per_node'    ] is not None
        assert self['threads_per_core' ] is not None
        assert self['n_partitions'     ] > 0


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
    def __init__(self, cfg, rcfg, log, prof):

        self.name  = type(self).__name__
        self._cfg  = cfg
        self._rcfg = rcfg
        self._log  = log
        self._prof = prof

        self._log.info('configuring RM %s', self.name)

        reg     = ru.zmq.RegistryClient(url=self._cfg.reg_addr)
        rm_info = reg.get('rm.%s' % self.name.lower())

        from_registry = bool(rm_info)

        if from_registry:

            self._log.debug('RM init from registry')
            rm_info = RMInfo(rm_info)
            rm_info.verify()

        else:
            self._log.debug('RM init from scratch')

            # let the base class collect some data, then let the impl take over
            rm_info = self._init_from_scratch()
            rm_info.verify()

            # have a valid info - store in registry and complete initialization
            reg.put('rm.%s' % self.name.lower(), rm_info.as_dict())

        reg.close()
        self._set_info(rm_info)


        # immediately set the network interface if it was configured
        # NOTE: setting this here implies that no ZMQ connectio was set up
        #       before the ResourceManager got created!
        if rm_info.details.get('network'):
            rc_cfg = ru.config.DefaultConfig()
            rc_cfg.iface = rm_info.details['network']

        # set up launch methods even when initialized from registry info.  In
        # that case, the LM *SHOULD NOT* be re-initialized, but only pick up
        # information from rm_info.
        self._prepare_launch_methods()


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
    def init_from_scratch(self, rm_info: RMInfo) -> RMInfo:
        '''
        This method MUST be overloaded by any RM implementation.  It will be
        called during `init_from_scratch` and is expected to check and correct
        or complete node information, such as `cores_per_node`, `gpus_per_node`
        etc., and to provide `rm_info.node_list` of the following form:

            node_list = [
                {
                    'name' : str                        # node name
                    'index': int                        # node index
                    'cores': [rpc.FREE, rpc.FREE, ...]  # cores per node
                    'gpus' : [rpc.FREE, rpc.FREE, ...]  # gpus per node
                    'lfs'  : int                        # lfs per node (MB)
                    'mem'  : int                        # mem per node (MB)
                },
                ...
            ]

        The node entries can be augmented with additional information which may
        be interpreted by the specific agent scheduler instance.
        '''

        raise NotImplementedError('init_from_scratch is not implemented')


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self):

        sys_arch = self._rcfg.get('system_architecture', {})
        rm_info  = RMInfo()

        # fill well defined default attributes
        rm_info.backup_nodes     = self._cfg.backup_nodes
        rm_info.requested_nodes  = self._cfg.nodes
        rm_info.requested_cores  = self._cfg.cores
        rm_info.requested_gpus   = self._cfg.gpus
        rm_info.cores_per_node   = self._cfg.cores_per_node
        rm_info.gpus_per_node    = self._cfg.gpus_per_node
        rm_info.lfs_per_node     = self._cfg.lfs_size_per_node
        rm_info.lfs_path         = ru.expand_env(self._cfg.lfs_path_per_node)

        rm_info.threads_per_gpu  = 1
        rm_info.mem_per_gpu      = None
        rm_info.mem_per_node     = self._rcfg.mem_per_node    or 0
        rm_info.numa_domain_map  = self._rcfg.numa_domain_map or {}
        rm_info.n_partitions     = self._rcfg.n_partitions
        rm_info.threads_per_core = int(os.environ.get('RADICAL_SMT') or
                                       sys_arch.get('smt', 1))

        rm_info.details = {
                'exact'        : sys_arch.get('exclusive'    , False),
                'n_partitions' : sys_arch.get('n_partitions' , 1),
                'oversubscribe': sys_arch.get('oversubscribe', False),
                'network'      : sys_arch.get('iface'        , None),
        }

        # let the specific RM instance fill out the RMInfo attributes
        rm_info = self.init_from_scratch(rm_info)

        # we expect to have a valid node list now
        self._log.info('node list: %s', rm_info.node_list)

        # the config can override core and gpu detection,
        # and decide to block some resources (part of the core specialization)
        blocked_cores = sys_arch.get('blocked_cores', [])
        blocked_gpus  = sys_arch.get('blocked_gpus',  [])

        self._log.info('blocked cores: %s' % blocked_cores)
        self._log.info('blocked gpus : %s' % blocked_gpus)

        if blocked_cores or blocked_gpus:

            rm_info.cores_per_node -= len(blocked_cores)
            rm_info.gpus_per_node  -= len(blocked_gpus)

            for node in rm_info.node_list:

                for idx in blocked_cores:
                    assert len(node['cores']) > idx
                    node['cores'][idx] = rpc.DOWN

                for idx in blocked_gpus:
                    assert len(node['gpus']) > idx
                    node['gpus'][idx] = rpc.DOWN

        # number of nodes could be unknown if `cores_per_node` is not in config,
        # but is provided by a corresponding RM in `init_from_scratch`
        if not rm_info.requested_nodes:
            n_nodes = rm_info.requested_cores / rm_info.cores_per_node
            if rm_info.gpus_per_node:
                n_nodes = max(
                    rm_info.requested_gpus / rm_info.gpus_per_node,
                    n_nodes)
            rm_info.requested_nodes = math.ceil(n_nodes)

        assert rm_info.requested_nodes <= len(rm_info.node_list)


        self._filter_nodes(rm_info)

        # add launch method information to rm_info
        rm_info.launch_methods = self._rcfg.launch_methods

        return rm_info


    # --------------------------------------------------------------------------
    #
    def _filter_nodes(self, rm_info: RMInfo) -> None:

        # if we have backup nodes, then check all nodes (including backup nodes)
        # to see if they are accessible.
        if rm_info.backup_nodes:
            procs = list()
            for node in rm_info.node_list:
                name = node['name']
                cmd  = 'ssh -oBatchMode=yes %s hostname' % name
                self._log.debug('check node: %s [%s]', name, cmd)
                proc = Process(cmd)
                proc.start()
                procs.append([name, proc, node])

            ok = list()
            for name, proc, node in procs:
                proc.wait(timeout=15)
                self._log.debug('check node: %s [%s]', name,
                                [proc.stdout, proc.stderr, proc.retcode])
                if proc.retcode is not None:
                    if not proc.retcode:
                        ok.append(node)
                else:
                    self._log.warning('check node: %s [%s] timed out',
                                      name, [proc.stdout, proc.stderr])
                    proc.cancel()
                    proc.wait(timeout=15)
                    if proc.retcode is None:
                        self._log.warning('check node: %s [%s] timed out again',
                                           name, [proc.stdout, proc.stderr])

            self._log.warning('using %d nodes out of %d', len(ok), len(procs))

            if not ok:
                raise RuntimeError('no accessible nodes found')

            # limit the nodelist to the requested number of nodes
            rm_info.node_list = ok


        # reduce the nodelist to the requested size
        rm_info.backup_list = list()
        if len(rm_info.node_list) > rm_info.requested_nodes:
            self._log.debug('reduce %d nodes to %d', len(rm_info.node_list),
                                                        rm_info.requested_nodes)
            rm_info.node_list   = rm_info.node_list[:rm_info.requested_nodes]
            rm_info.backup_list = rm_info.node_list[rm_info.requested_nodes:]

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

            assert agent_nodes == len(rm_info.agent_node_list)

        if service_nodes:

            if not rm_info.service_node_list:
                for _ in range(service_nodes):
                    rm_info.service_node_list.append(rm_info.node_list.pop())

            assert service_nodes == len(rm_info.service_node_list)

        self._log.info('compute nodes: %s' % len(rm_info.node_list))
        self._log.info('agent   nodes: %s' % len(rm_info.agent_node_list))
        self._log.info('service nodes: %s' % len(rm_info.service_node_list))

        # check if we can do any work
        if not rm_info.node_list:
            raise RuntimeError('ResourceManager has no nodes left to run tasks')



    # --------------------------------------------------------------------------
    #
    def _prepare_launch_methods(self):

        launch_methods     = self._rm_info.launch_methods
        self._launchers    = {}
        self._launch_order = launch_methods.get('order') or list(launch_methods)

        for lm_name in list(self._launch_order):

            lm_cfg = ru.Config(from_dict=launch_methods[lm_name])

            try:
                self._log.debug('prepare lm %s', lm_name)
                lm_cfg.pid           = self._cfg.pid
                lm_cfg.reg_addr      = self._cfg.reg_addr
                lm_cfg.resource      = self._cfg.resource
                self._launchers[lm_name] = rpa.LaunchMethod.create(
                    lm_name, lm_cfg, self._rm_info, self._log, self._prof)

            except Exception:
                self._log.exception('skip lm %s', lm_name)
                self._launch_order.remove(lm_name)

        self._log.info('launch methods: %s', self._launch_order)

        if not self._launchers:
            raise RuntimeError('no valid launch methods found')


    # --------------------------------------------------------------------------
    #
    def get_partition_ids(self):

        # TODO: this implies unique partition IDs across all launchers

        partition_ids = list()
        for lname in self._launchers:
            partition_ids += self._launchers[lname].get_partition_ids()

        return partition_ids


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the
    # ResourceManager.
    #
    @classmethod
    def create(cls, name, cfg, rcfg, log, prof):

        # Make sure that we are the base-class!
        if cls != ResourceManager:
            raise TypeError('ResourceManager Factory only available to base class!')

        rm = cls.get_manager(name)
        if rm is None:
            raise RuntimeError('ResourceManager %s unknown' % name)

        return rm(cfg, rcfg, log, prof)


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def get_manager(name):

        from .ccm     import CCM
        from .fork    import Fork
        from .lsf     import LSF
        from .pbspro  import PBSPro
        from .slurm   import Slurm
        from .torque  import Torque
        from .cobalt  import Cobalt
        from .yarn    import Yarn
        from .debug   import Debug

        impl = {
            RM_NAME_FORK   : Fork,
            RM_NAME_CCM    : CCM,
            RM_NAME_LSF    : LSF,
            RM_NAME_PBSPRO : PBSPro,
            RM_NAME_SLURM  : Slurm,
            RM_NAME_TORQUE : Torque,
            RM_NAME_COBALT : Cobalt,
            RM_NAME_YARN   : Yarn,
            RM_NAME_DEBUG  : Debug
        }

        return impl.get(name)


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def batch_started():
        '''
        Method determines from where it was called:
        either from the batch job or from outside (e.g., login node).
        '''

        return False


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
            self._log.debug('can launch %s with %s: %s',
                            task['uid'], name, lm_can_launch)

            if lm_can_launch:
                return launcher, name
            else:
                errors.append([name, err_message])

        self._log.error('no launch method for task %s:', task['uid'])
        for name, error in errors:
            self._log.debug('    %s: %s', name, error)

        return None, None


    # --------------------------------------------------------------------------
    #
    def get_launcher(self, lname):

        if lname not in self._launchers:
            raise ValueError('no such launcher %s' % lname)

        return self._launchers[lname]


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
                    assert ' ' not in node
                    if node in nodes: nodes[node] += 1
                    else            : nodes[node]  = 1

            if cpn:
                for node in list(nodes.keys()):
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

        # keep nodes to be indexed (node_index)
        # (required for jsrun ERF spec files and expanded to all other RMs)
        node_list = [{'name'  : node[0],
                      'index' : idx,
                      'cores' : [rpc.FREE] * node[1],
                      'gpus'  : [rpc.FREE] * rm_info.gpus_per_node,
                      'lfs'   : rm_info.lfs_per_node,
                      'mem'   : rm_info.mem_per_node}
                     for idx, node in enumerate(nodes)]

        return node_list


# ------------------------------------------------------------------------------

