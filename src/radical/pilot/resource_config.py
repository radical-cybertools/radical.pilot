
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'


from typing import Optional, List

import threading     as mt
import radical.utils as ru

from .constants import FREE, BUSY, DOWN


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
NUMA_DOMAINS_PER_NODE  = 'numa_domains_per_node'
SYSTEM_ARCHITECTURE    = 'system_architecture'
SCATTERED              = 'scattered'
SERVICES               = 'services'

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
class RaptorConfig(ru.TypedDict):

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
class AccessSchema(ru.TypedDict):

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
class ResourceConfig(ru.TypedDict):
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
        NUMA_DOMAINS_PER_NODE  : int         ,
        SYSTEM_ARCHITECTURE    : {str: None} ,
        SCATTERED              : bool        ,
        SERVICES               : [str]       ,

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
        SCHEMAS                : list()      ,
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
        NUMA_DOMAINS_PER_NODE  : 1           ,
        SYSTEM_ARCHITECTURE    : dict()      ,
        SCATTERED              : False       ,
        SERVICES               : list()      ,

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
class ResourceOccupation(ru.TypedDict):

    INDEX      = 'index'
    OCCUPATION = 'occupation'

    _schema = {
        INDEX     : int,
        OCCUPATION: float,
    }

    _defaults = {
        INDEX     : 0,
        OCCUPATION: FREE,
    }

    def __init__(self, from_dict: Optional[dict] = None,
                       **kwargs) -> None:

        super().__init__(from_dict=from_dict, **kwargs)


    def __str__(self):
        if self.occupation == DOWN:
            return '%d:----' % self.index
        return '%d:%.2f' % (self.index, self.occupation)


_RO = ResourceOccupation


# ------------------------------------------------------------------------------
#
class RankRequirements(ru.TypedDict):

    N_CORES         = 'n_cores'
    CORE_OCCUPATION = 'core_occupation'
    N_GPUS          = 'n_gpus'
    GPU_OCCUPATION  = 'gpu_occupation'
    LFS             = 'lfs'
    MEM             = 'mem'

    _schema = {
        N_CORES        : int,
        CORE_OCCUPATION: float,
        N_GPUS         : int,
        GPU_OCCUPATION : float,
        LFS            : int,
        MEM            : int,
    }

    _defaults = {
        N_CORES        : 1,
        CORE_OCCUPATION: BUSY,
        N_GPUS         : 0,
        GPU_OCCUPATION : BUSY,
        LFS            : 0,
        MEM            : 0,
    }

    def __str__(self):
        return 'RR(%d:%.2f, %d:%.2f, %d, %d)' % (
                self.n_cores, self.core_occupation,
                self.n_gpus,  self.gpu_occupation,
                self.lfs,     self.mem)


    def __eq__(self, other: 'RankRequirements') -> bool:

        if  self.n_cores         == self.n_cores           and \
            self.n_gpus          == self.n_gpus            and \
            self.lfs             == self.lfs               and \
            self.mem             == self.mem               and \
            self.core_occupation == self.core_occupation   and \
            self.gpu_occupation  == self.gpu_occupation:
            return True
        return False

    def __lt__(self, other: 'RankRequirements') -> bool:

        if  self.n_cores         <  other.n_cores          and \
            self.n_gpus          <  other.n_gpus           and \
            self.lfs             <  other.lfs              and \
            self.mem             <  other.mem              and \
            self.core_occupation <  other.core_occupation  and \
            self.gpu_occupation  <  other.gpu_occupation:
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

        if  self.n_cores         >  other.n_cores          and \
            self.n_gpus          >  other.n_gpus           and \
            self.lfs             >  other.lfs              and \
            self.mem             >  other.mem              and \
            self.core_occupation >  other.core_occupation  and \
            self.gpu_occupation  >  other.gpu_occupation:
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
class Slot(ru.TypedDict):

    CORES       = 'cores'
    GPUS        = 'gpus'
    LFS         = 'lfs'
    MEM         = 'mem'
    NODE_INDEX  = 'node_index'
    NODE_NAME   = 'node_name'

    _schema = {
        CORES      : [_RO],  # list of tuples [(core_id, core_occupation), ...]
        GPUS       : [_RO],  # list of tuples [(gpu_id,  core_occupation), ...]
        LFS        : int,
        MEM        : int,
        NODE_INDEX : int,
        NODE_NAME  : str,
    }

    _defaults = {
        CORES      : list(),
        GPUS       : list(),
        LFS        : 0,
        MEM        : 0,
        NODE_INDEX : 0,
        NODE_NAME  : '',
    }

    def __init__(self, from_dict: dict = None, **kwargs):

        if not from_dict:
            from_dict = kwargs

        if from_dict:

            cores = from_dict.get('cores')
            gpus  = from_dict.get('gpus')

            if cores:
                if not isinstance(cores[0], _RO):
                    from_dict['cores'] = [_RO(index=i) for i in cores]

            if gpus:
                if not isinstance(gpus[0], _RO):
                    from_dict['gpus'] = [_RO(index=i) for i in gpus]

        super().__init__(from_dict, **kwargs)


# ------------------------------------------------------------------------------
#
class NodeResources(ru.TypedDict):
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
        CORES    : [_RO],
        GPUS     : [_RO],
        LFS      : int,
        MEM      : int,
    }

    _defaults = {
        INDEX    : 0,
        NAME     : '',
        CORES    : list(),
        GPUS     : list(),
        LFS      : 0,
        MEM      : 0,
    }

    def __init__(self, from_dict: dict):

        self.__lock__ = mt.RLock()

        cores = from_dict.get('cores')
        gpus  = from_dict.get('gpus')

        if cores:
            if not isinstance(cores[0], _RO):
                from_dict['cores'] = [_RO(index=i,occupation=o)
                                                    for i,o in enumerate(cores)]

        if gpus:
            if not isinstance(gpus[0], _RO):
                from_dict['gpus'] = [_RO(index=i,occupation=o)
                                                     for i,o in enumerate(gpus)]

        super().__init__(from_dict)


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
                    cores = [_RO(index=core) for core in cores]

                if gpus and isinstance(gpus[0], int):
                    gpus = [_RO(index=gpu) for gpu in gpus]

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
          # print('      -> %s' % self)
            for ro in cores:
                self.cores[ro.index].occupation += ro.occupation

            for ro in gpus:
                self.gpus[ro.index].occupation += ro.occupation

            self.lfs -= lfs
            self.mem -= mem

          # print('      => %s' % self)
          # print('allocate %s' % slot)
          # print()


    # --------------------------------------------------------------------------
    #
    def deallocate_slot(self, slot : 'Slot') -> None:

        with self.__lock__:

          # print('release  %s' % slot)
          # print('     ->  %s' % self)
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

          # print('     =>  %s' % self)


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
                        cores.append(_RO(index=ro.index,
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
                        gpus.append(_RO(index=ro.index,
                                        occupation=rr.gpu_occupation))
                    if len(gpus) == rr.n_gpus:
                        break

                if len(gpus) < rr.n_gpus:
                    return None

            if rr.lfs and self.lfs < rr.lfs: return None
            if rr.mem and self.mem < rr.mem: return None

            slot = Slot(cores=cores, gpus=gpus, lfs=rr.lfs, mem=rr.mem,
                        node_index=self.index, node_name=self.name)
            self.allocate_slot(slot, _check=False)

            return slot


# ------------------------------------------------------------------------------
#
class NodeList(ru.TypedDict):

    NODES          = 'nodes'
    UNIFORM        = 'uniform'
    CORES_PER_NODE = 'cores_per_node'
    GPUS_PER_NODE  = 'gpus_per_node'
    LFS_PER_NODE   = 'lfs_per_node'
    MEM_PER_NODE   = 'mem_per_node'

    _schema = {
        NODES          : [NodeResources],
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
              # print('--- got slot %s' % slot)
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
          # print(' --- %5d' % count)
            return None

        self.__index__ = stop

      # print(' === %5d' % count)
        return slots


    def release_slots(self, slots: List[Slot]) -> None:

        for slot in slots:

            node = self.nodes[slot.node_index]
            node.deallocate_slot(slot)

        if self.__last_failed_rr__:
            self.__index__ = min([slot.node_index for slot in slots]) - 1

        self.__last_failed_rr__ = None
        self.__last_failed_n__  = None


# ------------------------------------------------------------------------------

