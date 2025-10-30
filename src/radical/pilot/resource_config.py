
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from typing import Optional, List

import threading     as mt

from .constants  import BUSY, DOWN
from .utils.misc import FastTypedDict


LABEL                  = 'label'
DESCRIPTION            = 'description'
NOTES                  = 'notes'
DEFAULT_SCHEMA         = 'default_schema'
SCHEMAS                = 'schemas'
JOB_MANAGER_ENDPOINT   = 'job_manager_endpoint'
JOB_MANAGER_HOP        = 'job_manager_hop'
FILESYSTEM_ENDPOINT    = 'filesystem_endpoint'
DEFAULT_REMOTE_WORKDIR = 'default_remote_workdir'
DEFAULT_QUEUE          = 'default_queue'
RESOURCE_MANAGER       = 'resource_manager'
AGENT_CONFIG           = 'agent_config'
AGENT_SCHEDULER        = 'agent_scheduler'
AGENT_SPAWNER          = 'agent_spawner'
PRE_BOOTSTRAP_0        = 'pre_bootstrap_0'
PRE_BOOTSTRAP_1        = 'pre_bootstrap_1'
RP_VERSION             = 'rp_version'
VIRTENV                = 'virtenv'
VIRTENV_MODE           = 'virtenv_mode'
PYTHON_DIST            = 'python_dist'
PYTHON_INTERPRETER     = 'python_interpreter'
LAUNCH_METHODS         = 'launch_methods'
LFS_PATH_PER_NODE      = 'lfs_path_per_node'
LFS_SIZE_PER_NODE      = 'lfs_size_per_node'
TASK_TMP               = 'task_tmp'
MEM_PER_NODE           = 'mem_per_node'
CORES_PER_NODE         = 'cores_per_node'
GPUS_PER_NODE          = 'gpus_per_node'
NUMA_DOMAIN_MAP        = 'numa_domain_map'
N_PARTITIONS           = 'n_partitions'
NETWORK_INTERFACE      = 'network_interface'
SYSTEM_ARCHITECTURE    = 'system_architecture'
SCATTERED              = 'scattered'

FAKE_RESOURCES         = 'fake_resources'
MANDATORY_ARGS         = 'mandatory_args'
FORWARD_TUNNEL_ENDPOINT = 'forward_tunnel_endpoint'

NEW_SESSION_PER_TASK   = 'new_session_per_task'
TASK_PRE_LAUNCH        = 'task_pre_launch'
TASK_POST_LAUNCH       = 'task_post_launch'
TASK_PRE_EXEC          = 'task_pre_exec'
TASK_POST_EXEC         = 'task_post_exec'

RAPTOR                 = 'raptor'
RAPTOR_HB_DELAY        = 'hb_delay'
RAPTOR_HB_TIMEOUT      = 'hb_timeout'
RAPTOR_HB_FREQUENCY    = 'hb_frequency'

ENDPOINTS_DEFAULT      = {JOB_MANAGER_ENDPOINT: 'fork://localhost/',
                          FILESYSTEM_ENDPOINT : 'file://localhost/'}


# ------------------------------------------------------------------------------
#
class RaptorConfig(FastTypedDict):

    _schema = {
        RAPTOR_HB_DELAY    : int,
        RAPTOR_HB_TIMEOUT  : int,
        RAPTOR_HB_FREQUENCY: int,
    }

    _defaults = {
        RAPTOR_HB_DELAY    : 5,
        RAPTOR_HB_TIMEOUT  : 500,
        RAPTOR_HB_FREQUENCY: 1000,
    }


# ------------------------------------------------------------------------------
#
class AccessSchema(FastTypedDict):

    _schema = {
        JOB_MANAGER_ENDPOINT: str,
        JOB_MANAGER_HOP     : str,
        FILESYSTEM_ENDPOINT : str,
    }

    _defaults = {
        JOB_MANAGER_ENDPOINT: None,
        JOB_MANAGER_HOP     : None,
        FILESYSTEM_ENDPOINT : None,
    }


# ------------------------------------------------------------------------------
#
class ResourceConfig(FastTypedDict):
    '''
    docstrings goes here
    '''

    _schema = {
        LABEL                  : str         ,
        DESCRIPTION            : str         ,
        NOTES                  : str         ,
        DEFAULT_SCHEMA         : str         ,
        SCHEMAS                : {str: AccessSchema},
        RAPTOR                 : RaptorConfig,

        # FIXME: AM - need to resolve since  in Session it is moved into RD
        #        `_get_resource_sandbox` ->  `KeyError: 'filesystem_endpoint'`
        JOB_MANAGER_ENDPOINT   : str         ,
        JOB_MANAGER_HOP        : str         ,
        FILESYSTEM_ENDPOINT    : str         ,

        DEFAULT_REMOTE_WORKDIR : str         ,
        DEFAULT_QUEUE          : str         ,
        RESOURCE_MANAGER       : str         ,
        AGENT_CONFIG           : str         ,
        AGENT_SCHEDULER        : str         ,
        AGENT_SPAWNER          : str         ,
        PRE_BOOTSTRAP_0        : [str]       ,
        PRE_BOOTSTRAP_1        : [str]       ,
        RP_VERSION             : str         ,
        VIRTENV                : str         ,
        VIRTENV_MODE           : str         ,
        PYTHON_DIST            : str         ,
        PYTHON_INTERPRETER     : str         ,
        LAUNCH_METHODS         : {str: None} ,
        LFS_PATH_PER_NODE      : str         ,
        LFS_SIZE_PER_NODE      : int         ,
        TASK_TMP               : str         ,
        MEM_PER_NODE           : int         ,
        CORES_PER_NODE         : int         ,
        GPUS_PER_NODE          : int         ,
        NUMA_DOMAIN_MAP        : {int: None} ,
        N_PARTITIONS           : int         ,
        NETWORK_INTERFACE      : str         ,
        SYSTEM_ARCHITECTURE    : {str: None} ,
        SCATTERED              : bool        ,

        FAKE_RESOURCES         : bool        ,
        MANDATORY_ARGS         : [str]       ,
        FORWARD_TUNNEL_ENDPOINT: str         ,
        NEW_SESSION_PER_TASK   : bool        ,
        TASK_PRE_LAUNCH        : [str]       ,
        TASK_POST_LAUNCH       : [str]       ,
        TASK_PRE_EXEC          : [str]       ,
        TASK_POST_EXEC         : [str]       ,
    }

    _defaults = {
        LABEL                  : ''          ,
        DESCRIPTION            : ''          ,
        NOTES                  : ''          ,
        DEFAULT_SCHEMA         : ''          ,
        SCHEMAS                : dict()      ,
        RAPTOR                 : RaptorConfig(),

        # FIXME: AM - need to resolve since in Session it is moved into RD
        #        `_get_resource_sandbox` -> `KeyError: 'filesystem_endpoint'`
        JOB_MANAGER_ENDPOINT   : None        ,
        JOB_MANAGER_HOP        : None        ,
        FILESYSTEM_ENDPOINT    : None        ,

        DEFAULT_REMOTE_WORKDIR : ''          ,
        DEFAULT_QUEUE          : ''          ,
        RESOURCE_MANAGER       : ''          ,
        AGENT_CONFIG           : 'default'   ,
        AGENT_SCHEDULER        : 'CONTINUOUS',
        AGENT_SPAWNER          : 'POPEN'     ,
        PRE_BOOTSTRAP_0        : list()      ,
        PRE_BOOTSTRAP_1        : list()      ,
        RP_VERSION             : 'installed' ,
        VIRTENV                : ''          ,
        VIRTENV_MODE           : 'local'     ,
        PYTHON_DIST            : 'default'   ,
        PYTHON_INTERPRETER     : ''          ,
        LAUNCH_METHODS         : dict()      ,
        LFS_PATH_PER_NODE      : ''          ,
        LFS_SIZE_PER_NODE      : 0           ,
        TASK_TMP               : ''          ,
        MEM_PER_NODE           : 0           ,
        CORES_PER_NODE         : 0           ,
        GPUS_PER_NODE          : 0           ,
        NUMA_DOMAIN_MAP        : dict()      ,
        N_PARTITIONS           : 1           ,
        NETWORK_INTERFACE      : ''          ,
        SYSTEM_ARCHITECTURE    : dict()      ,
        SCATTERED              : True        ,

        FAKE_RESOURCES         : False       ,
        MANDATORY_ARGS         : list()      ,
        FORWARD_TUNNEL_ENDPOINT: ''          ,
        NEW_SESSION_PER_TASK   : True        ,
        TASK_PRE_LAUNCH        : list()      ,
        TASK_POST_LAUNCH       : list()      ,
        TASK_PRE_EXEC          : list()      ,
        TASK_POST_EXEC         : list()      ,
    }


# ------------------------------------------------------------------------------
#
class RO(FastTypedDict):

    INDEX      = 'index'
    OCCUPATION = 'occupation'

    _schema = {
        INDEX     : int,
        OCCUPATION: float,
    }

    _defaults = {
        INDEX     : 0,
        OCCUPATION: None,
    }

    def __init__(self, from_dict: Optional[dict] = None,
                       **kwargs) -> None:

        super().__init__(from_dict=from_dict, **kwargs)


    def __str__(self):
        if self.occupation == DOWN:
            return '%d:----' % self.index
        return '%d:%.2f' % (self.index, self.occupation)

    def _verify(self):

        if self.occupation is None:
            raise ValueError('missing occupation: %s' % self.occupation)


ResourceOccupation = RO


# ------------------------------------------------------------------------------
#
class RankRequirements(FastTypedDict):

    N_CORES         = 'n_cores'
    CORE_OCCUPATION = 'core_occupation'
    N_GPUS          = 'n_gpus'
    GPU_OCCUPATION  = 'gpu_occupation'
    LFS             = 'lfs'
    MEM             = 'mem'
    NUMA            = 'numa'

    _schema = {
        N_CORES        : int,
        CORE_OCCUPATION: float,
        N_GPUS         : int,
        GPU_OCCUPATION : float,
        LFS            : int,
        MEM            : int,
        NUMA           : bool,
    }

    _defaults = {
        N_CORES        : 1,
        CORE_OCCUPATION: BUSY,
        N_GPUS         : 0,
        GPU_OCCUPATION : BUSY,
        LFS            : 0,
        MEM            : 0,
        NUMA           : False,
    }

    def __str__(self):
        return 'RR(%d:%.2f, %d:%.2f, %d, %d [%s])' % (
                self.n_cores, self.core_occupation,
                self.n_gpus,  self.gpu_occupation,
                self.lfs,     self.mem,
                'numa' if self.numa else 'non-numa')


    # comparison operators
    #
    # NOTE: we are not really interested in strict ordering here,
    #       but we do care about *fast* and approximate comparisons.
    #       'larger' in general means 'more or more specialized resources'.
    #
    # NOTE: NUMA setting is not considered for comparison
    #
    def __eq__(self, other: 'RankRequirements') -> bool:

        if  self.n_cores         == other.n_cores          and \
            self.n_gpus          == other.n_gpus           and \
            self.lfs             == other.lfs              and \
            self.mem             == other.mem              and \
            self.core_occupation == other.core_occupation  and \
            self.gpu_occupation  == other.gpu_occupation   :
            return True
        return False

    def __lt__(self, other: 'RankRequirements') -> bool:

        if  self.n_cores         <= other.n_cores          and \
            self.n_gpus          <= other.n_gpus           and \
            self.lfs             <= other.lfs              and \
            self.mem             <= other.mem              and \
            self.core_occupation <= other.core_occupation  and \
            self.gpu_occupation  <= other.gpu_occupation:
            return True
        return False

    def __le__(self, other: 'RankRequirements') -> bool:

        if  self.n_cores         <= other.n_cores          and \
            self.n_gpus          <= other.n_gpus           and \
            self.lfs             <= other.lfs              and \
            self.mem             <= other.mem              and \
            self.core_occupation <= other.core_occupation  and \
            self.gpu_occupation  <= other.gpu_occupation:
            return True
        return False

    def __gt__(self, other: 'RankRequirements') -> bool:

        if  self.n_cores         >= other.n_cores          and \
            self.n_gpus          >= other.n_gpus           and \
            self.lfs             >= other.lfs              and \
            self.mem             >= other.mem              and \
            self.core_occupation >= other.core_occupation  and \
            self.gpu_occupation  >= other.gpu_occupation:
            return True
        return False

    def __ge__(self, other: 'RankRequirements') -> bool:

        if  self.n_cores         >= other.n_cores          and \
            self.n_gpus          >= other.n_gpus           and \
            self.lfs             >= other.lfs              and \
            self.mem             >= other.mem              and \
            self.core_occupation >= other.core_occupation  and \
            self.gpu_occupation  >= other.gpu_occupation:
            return True
        return False


# ------------------------------------------------------------------------------
#
class Slot(FastTypedDict):

    CORES       = 'cores'
    GPUS        = 'gpus'
    LFS         = 'lfs'
    MEM         = 'mem'
    NODE_INDEX  = 'node_index'
    NODE_NAME   = 'node_name'
    VERSION     = 'version'  # use this to distinguish from old slot structure

    _schema = {
        CORES      : [RO],  # list of tuples [(core_id, core_occupation), ...]
        GPUS       : [RO],  # list of tuples [(gpu_id,  core_occupation), ...]
        LFS        : int,
        MEM        : int,
        NODE_INDEX : int,
        NODE_NAME  : str,
        VERSION    : int,
    }

    _defaults = {
        CORES      : list(),
        GPUS       : list(),
        LFS        : 0,
        MEM        : 0,
        NODE_INDEX : 0,
        NODE_NAME  : '',
        VERSION    : 1,
    }

    def __init__(self, from_dict: dict = None, **kwargs):

        if not from_dict:
            from_dict = kwargs

        if from_dict:

            cores = from_dict.get('cores')
            gpus  = from_dict.get('gpus')

            if cores:
                # this is much faster than `isinstance`
                if cores[0].__class__.__name__ == 'dict':
                    from_dict['cores'] =  [RO(d) for d in cores]

                elif isinstance(cores[0], int):
                    from_dict['cores'] =  [RO(index=i, occupation=BUSY)
                                                 for i in cores]

            if gpus:
                if gpus[0].__class__.__name__ == 'dict':
                    from_dict['gpus'] =  [RO(d) for d in gpus]

                elif isinstance(gpus[0], int):
                    from_dict['gpus'] =  [RO(index=i, occupation=BUSY)
                                                for i in gpus]


        super().__init__(from_dict, **kwargs)


# ------------------------------------------------------------------------------
#
class Node(FastTypedDict):
    '''
    Node resources as reported by the resource manager, used by the scheduler
    '''

    INDEX    = 'index'
    NAME     = 'name'
    CORES    = 'cores'
    GPUS     = 'gpus'
    LFS      = 'lfs'
    MEM      = 'mem'

    _schema = {
        INDEX    : int,
        NAME     : str,
        CORES    : [RO],
        GPUS     : [RO],
        LFS      : int,
        MEM      : int,
    }

    _defaults = {
        INDEX    : 0,
        NAME     : '',
        CORES    : list(),
        GPUS     : list(),
        LFS      : None,
        MEM      : None,
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict: dict):

        self.__lock__ = mt.RLock()

        cores = from_dict.get('cores')
        gpus  = from_dict.get('gpus')

        if cores:
            if not isinstance(cores[0], RO):
                from_dict['cores'] = [RO(index=i, occupation=o)
                                                    for i,o in enumerate(cores)]

        if gpus:
            if not isinstance(gpus[0], RO):
                from_dict['gpus'] = [RO(index=i, occupation=o)
                                                     for i,o in enumerate(gpus)]

        super().__init__(from_dict)


    # --------------------------------------------------------------------------
    #
    def _get_core_index(self, ro):

        for i, _ro in enumerate(self.cores):
            if _ro.index == ro.index:
                return i

        raise ValueError('invalid core index %s' % ro.index)


    # --------------------------------------------------------------------------
    #
    def _get_gpu_index(self, ro):

        for i, _ro in enumerate(self.gpus):
            if _ro.index == ro.index:
                return i

        raise ValueError('invalid gpu index %s' % ro.index)


    # --------------------------------------------------------------------------
    #
    def allocate_slot(self, slot  : Slot,
                            _check: bool = True) -> None:

        cores = slot.cores
        gpus  = slot.gpus
        lfs   = slot.lfs
        mem   = slot.mem

        if _check:
            assert self.index == slot.node_index
            assert self.name  == slot.node_name

        with self.__lock__:

            # we only need to perform consistency checks for slots which were not
            # created by `self.find_slot`
            if _check:

                # we allow for core indexes but convert into full occupancy then
                if cores and isinstance(cores[0], int):
                    cores = [RO(index=core, occupation=BUSY) for core in cores]

                if gpus and isinstance(gpus[0], int):
                    gpus = [RO(index=gpu, occupation=BUSY) for gpu in gpus]

                # make sure the selected cores exist, are not down, and
                # occupancy is compatible with the request
                for ro in cores:
                    assert ro.index < len(self.cores)
                    ro_available = BUSY - self.cores[ro.index].occupation
                    assert ro_available >= ro.occupation
                  # # DOWN check is covered by occupancy check
                  # assert self.cores[ro.index].occupation is not DOWN, \
                  #         'core %d is down' % ro.index

                for ro in gpus:
                    assert ro.index < len(self.gpus)
                    ro_available = BUSY - self.gpus[ro.index].occupation
                    assert ro_available >= ro.occupation
                  # # DOWN check is covered by occupancy check
                  # assert self.gpus[ro.index].occupation is not DOWN, \
                  #         'gpu %d is down' % ro.index

                if lfs: assert self.lfs >= lfs
                if mem: assert self.mem >= mem

            # slot is valid = apply the respective changes
            for ro in cores:
                c_idx = self._get_core_index(ro)
                self.cores[c_idx].occupation += ro.occupation

            for ro in gpus:
                g_idx = self._get_gpu_index(ro)
                self.gpus[g_idx].occupation += ro.occupation

            if self.lfs is not None: self.lfs -= lfs
            if self.mem is not None: self.mem -= mem


    # --------------------------------------------------------------------------
    #
    def deallocate_slot(self, slot : 'Slot') -> None:

        with self.__lock__:

            for ro in slot.cores:
                self.cores[ro.index].occupation -= ro.occupation
              # assert self.cores[ro.index].occupation >= 0.0, \
              #         'invalid core release: %s' % self

            for ro in slot.gpus:
                self.gpus[ro.index].occupation -= ro.occupation
              # assert self.gpus[ro.index].occupation >= 0.0, \
              #         'invalid gpu release: %s' % self

            self.lfs += slot.lfs
            self.mem += slot.mem


    # --------------------------------------------------------------------------
    #
    def find_slot(self, rr: RankRequirements) -> Optional[Slot]:

        with self.__lock__:

            cores = list()
            gpus  = list()

            # NOTE: the current mechanism will never use the same core or gpu
            #       multiple times for the created slot, even if the respective
            #       occupation would allow for it.
            if rr.n_cores:
                for ro in self.cores:
                    if ro.occupation is DOWN:
                        continue
                    if rr.core_occupation <= BUSY - ro.occupation:
                        cores.append(RO(index=ro.index,
                                        occupation=rr.core_occupation))
                    if len(cores) == rr.n_cores:
                        break

                if len(cores) < rr.n_cores:
                    return None

            if rr.n_gpus:
                for ro in self.gpus:
                    if ro.occupation is DOWN:
                        continue
                    if rr.gpu_occupation <= BUSY - ro.occupation:
                        gpus.append(RO(index=ro.index,
                                       occupation=rr.gpu_occupation))
                    if len(gpus) == rr.n_gpus:
                        break

                if len(gpus) < rr.n_gpus:
                    return None

            if self.lfs is not None:
                if rr.lfs and self.lfs < rr.lfs: return None

            if self.mem is not None:
                if rr.mem and self.mem < rr.mem: return None

            slot = Slot(cores=cores, gpus=gpus, lfs=rr.lfs, mem=rr.mem,
                        node_index=self.index, node_name=self.name)
            self.allocate_slot(slot, _check=False)

            return slot


# ------------------------------------------------------------------------------
#
class NodeList(FastTypedDict):

    NODES          = 'nodes'
    UNIFORM        = 'uniform'
    CORES_PER_NODE = 'cores_per_node'
    GPUS_PER_NODE  = 'gpus_per_node'
    LFS_PER_NODE   = 'lfs_per_node'
    MEM_PER_NODE   = 'mem_per_node'

    _schema = {
        NODES          : [Node],
        UNIFORM        : bool,
        CORES_PER_NODE : int,
        GPUS_PER_NODE  : int,
        LFS_PER_NODE   : int,
        MEM_PER_NODE   : int,

    }

    _defaults = {
        NODES          : list(),
        UNIFORM        : True,
        CORES_PER_NODE : None,
        GPUS_PER_NODE  : None,
        LFS_PER_NODE   : None,
        MEM_PER_NODE   : None,
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict: dict = None, **kwargs) -> None:

        super().__init__(from_dict=from_dict, **kwargs)

        self.__index__          = int()
        self.__last_failed_rr__ = None
        self.__last_failed_n__  = None

        self.__verified__ = False


    # --------------------------------------------------------------------------
    #
    def verify(self) -> None:

        if not self.nodes:
            return

        self.uniform = True
        node_0  = self.nodes[0]
        for node in self.nodes[1:]:

            if node.cores != node_0.cores or \
               node.gpus  != node_0.gpus  or \
               node.lfs   != node_0.lfs   or \
               node.mem   != node_0.mem:
                self.uniform = False
                break

        if self.uniform:
            self.cores_per_node = len(node_0.cores)
            self.gpus_per_node  = len(node_0.gpus)
            self.lfs_per_node   = node_0.lfs
            self.mem_per_node   = node_0.mem

        else:
            self.cores_per_node = None
            self.gpus_per_node  = None
            self.lfs_per_node   = None
            self.mem_per_node   = None

        self.__nodes_by_name__  = {node.name : node for node in self.nodes}

        self.__verified__ = True


    # --------------------------------------------------------------------------
    #
    def _assert_rr(self, rr: RankRequirements, n_slots:int) -> None:

        if not self.__verified__:
            self.verify()

        if not self.uniform:
            raise RuntimeError('verification unsupported for non-uniform nodes')

        if not rr.n_cores:
            raise ValueError('invalid rank requirements: %s' % rr)

        ranks_per_node = self.cores_per_node / rr.n_cores

        if rr.n_gpus:
            ranks_per_node = min(ranks_per_node, self.gpus_per_node / rr.n_gpus)

        if rr.lfs:
            ranks_per_node = min(ranks_per_node, self.lfs_per_node / rr.lfs)

        if rr.mem:
            ranks_per_node = min(ranks_per_node, self.mem_per_node / rr.mem)

        if ranks_per_node < 1:
            raise ValueError('invalid rank requirements: %s' % rr)

        if n_slots > len(self.nodes) * ranks_per_node:
            raise ValueError('invalid rank requirements: %s' % rr)


    # --------------------------------------------------------------------------
    #
    def find_slots(self, rr: RankRequirements, n_slots:int = 1) -> List[Slot]:

        self._assert_rr(rr, n_slots)

        if self.__last_failed_rr__:
            if self.__last_failed_rr__ >= rr and \
               self.__last_failed_n__  >= n_slots:
                return None

        slots = list()
        count = 0
        start = self.__index__

        for i in range(0, len(self.nodes)):

            idx  = (start + i) % len(self.nodes)
            node = self.nodes[idx]

            while True:
                count += 1
                slot = node.find_slot(rr)
                if not slot:
                    break

                slots.append(slot)
                if len(slots) == n_slots:
                    stop = idx
                    break

            if len(slots) == n_slots:
                break

        if len(slots) != n_slots:
            # free whatever we got
            for slot in slots:
                node = self.nodes[slot.node_index]
                node.deallocate_slot(slot)
            self.__last_failed_rr__ = rr
            self.__last_failed_n__  = n_slots
            return None

        self.__index__ = stop

        return slots


    # --------------------------------------------------------------------------
    #
    def release_slots(self, slots: List[Slot]) -> None:

        for slot in slots:

            node = self.nodes[slot.node_index]
            node.deallocate_slot(slot)

        if self.__last_failed_rr__:
            self.__index__ = min([slot.node_index for slot in slots]) - 1

        self.__last_failed_rr__ = None
        self.__last_failed_n__  = None


# ------------------------------------------------------------------------------
#
class NumaDomain(FastTypedDict):
    '''
    A `NumaDomain` defines what cores and GPUs on a node are part of the same
    momory domain.
    '''

    CORES = 'cores'
    GPUS  = 'gpus'

    _schema = {
            CORES: [int],
            GPUS : [int],
    }

    _defaults = {
            CORES: list(),
            GPUS : list(),
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, cores, gpus):

        super().__init__(cores=cores, gpus=gpus)



# ------------------------------------------------------------------------------
#
class NumaDomainMap(FastTypedDict):
    '''
    The `NumaDomainMap` is a dictionary of `NumaDomain`s which are mapped to the
    Node's NUMA domain indizes.  For example, a node with 8 cores and 2 GPUs
    could be split into two NUMA domains with 4 cores and 1 GPU each.  The
    `NumaDomainMap` would then look like this:

        {
           0: NumaDomain(cores=[0, 1, 2, 3], gpus=[0]),
           1: NumaDomain(cores=[4, 5, 6, 7], gpus=[1]),
        }
    '''

    pass


# ------------------------------------------------------------------------------
#
class NumaNode(Node):
    '''
    `NumaNode` is a specialization of `Node` which is aware of NUMA domains.
    It is used by the scheduler to allocate resources in a NUMA aware way.
    The `NumaNode` instance behaves like a regular `Node` instance if no
    `numa_domain_map` is provided.
    '''

    NUMA_DOMAINS = 'numa_domains'

    _schema = {
        NUMA_DOMAINS: {1: NumaDomain},
    }

    _defaults = {
        NUMA_DOMAINS: dict(),
    }


    # --------------------------------------------------------------------------
    #
    def as_dict(self, *args, **kwargs):

        ret = super().as_dict(*args, **kwargs)

        if ret.get('numa_domains'):
            del ret['cores']
            del ret['gpus']

        return ret


    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict: dict, numa_domain_map: dict = None) -> None:

        super().__init__(from_dict)

        if not numa_domain_map:

            # this instance behaves like a regular Node instance
            self.numa_domains = None

        else:

            # otherwise we fill the `numa_domains` map with virtual node instances,
            # one per NUMA domain
            for domain_id, domain_descr in numa_domain_map.items():

                n = Node(from_dict)
                n.name  = '%s.%s' % (self.name, domain_id)

                # the resource occupations are *shared* between the node (base
                # class) which does non-numa scheduling, and the numa domains.
                n.cores = [self.cores[i] for i in domain_descr.cores]
                n.gpus  = [self.gpus[i]  for i in domain_descr.gpus]

                # LFS and MEM are not split across NUMA domains and are always
                # allocated on node level
                n.lfs   = None
                n.mem   = None

                self.numa_domains[domain_id] = n


    # --------------------------------------------------------------------------
    #
    def find_slot(self, rr: RankRequirements) -> Optional[Slot]:

        if not rr.numa:
            return super().find_slot(rr)

        if not self.numa_domains:
            return super().find_slot(rr)

        for nd in self.numa_domains.values():

            # first find `lfs` and `mem` on the node level
            if self.lfs is not None:
                if rr.lfs and self.lfs < rr.lfs: return None

            if self.mem is not None:
                if rr.mem and self.mem < rr.mem: return None

            # then find cores and gpus on the numa domain level
            for nd in self.numa_domains.values():
                slot = nd.find_slot(rr)
                if slot:
                    return slot

# ------------------------------------------------------------------------------
#
class NumaNodeList(NodeList):


    NUMA_NODES = 'numa_nodes'

    _schema = {
        NUMA_NODES: [NumaNode],
    }


# ------------------------------------------------------------------------------

