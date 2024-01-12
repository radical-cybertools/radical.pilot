
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

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
VIRTENV_DIST           = 'virtenv_dist'
PYTHON_DIST            = 'python_dist'
LAUNCH_METHODS         = 'launch_methods'
LFS_PATH_PER_NODE      = 'lfs_path_per_node'
LFS_SIZE_PER_NODE      = 'lfs_size_per_node'
MEM_PER_NODE           = 'mem_per_node'
CORES_PER_NODE         = 'cores_per_node'
GPUS_PER_NODE          = 'gpus_per_node'
SYSTEM_ARCHITECTURE    = 'system_architecture'

FAKE_RESOURCES         = 'fake_resources'
MANDATORY_ARGS         = 'mandatory_args'
FORWARD_TUNNEL_ENDPOINT = 'forward_tunnel_endpoint'

NEW_SESSION_PER_TASK   = 'new_session_per_task'
TASK_PRE_EXEC          = 'task_pre_exec'

ENDPOINTS_DEFAULT      = {JOB_MANAGER_ENDPOINT: 'fork://localhost/',
                          FILESYSTEM_ENDPOINT : 'file://localhost/'}

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
class ResourceDescription(ru.TypedDict):
    '''
    docstrings goes here
    '''

    _schema = {
        DESCRIPTION            : str        ,
        NOTES                  : str        ,
        DEFAULT_SCHEMA         : str        ,
        SCHEMAS                : {str: AccessSchema},

        # FIXME: AM - need to resolve since in Session it is moved into RD
        #        `_get_resource_sandbox` -> `KeyError: 'filesystem_endpoint'`
        JOB_MANAGER_ENDPOINT   : str        ,
        JOB_MANAGER_HOP        : str        ,
        FILESYSTEM_ENDPOINT    : str        ,

        DEFAULT_REMOTE_WORKDIR : str        ,
        DEFAULT_QUEUE          : str        ,
        RESOURCE_MANAGER       : str        ,
        AGENT_CONFIG           : str        ,
        AGENT_SCHEDULER        : str        ,
        AGENT_SPAWNER          : str        ,
        PRE_BOOTSTRAP_0        : [str]      ,
        PRE_BOOTSTRAP_1        : [str]      ,
        RP_VERSION             : str        ,
        VIRTENV                : str        ,
        VIRTENV_MODE           : str        ,
        VIRTENV_DIST           : str        ,
        PYTHON_DIST            : str        ,
        LAUNCH_METHODS         : {str: None},
        LFS_PATH_PER_NODE      : str        ,
        LFS_SIZE_PER_NODE      : str        ,
        MEM_PER_NODE           : int        ,
        CORES_PER_NODE         : int        ,
        GPUS_PER_NODE          : int        ,
        SYSTEM_ARCHITECTURE    : {str: None},

        FAKE_RESOURCES         : bool       ,
        MANDATORY_ARGS         : [str]      ,
        FORWARD_TUNNEL_ENDPOINT: str        ,
        NEW_SESSION_PER_TASK   : bool       ,
        TASK_PRE_EXEC          : [str]      ,
    }

    _defaults = {
        DESCRIPTION            : ''          ,
        NOTES                  : ''          ,
        DEFAULT_SCHEMA         : ''          ,
        SCHEMAS                : list()      ,

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
        PRE_BOOTSTRAP_0        : []          ,
        PRE_BOOTSTRAP_1        : []          ,
        RP_VERSION             : ''          ,
        VIRTENV                : ''          ,
        VIRTENV_MODE           : ''          ,
        VIRTENV_DIST           : ''          ,
        PYTHON_DIST            : 'default'   ,
        LAUNCH_METHODS         : {}          ,
        LFS_PATH_PER_NODE      : ''          ,
        LFS_SIZE_PER_NODE      : ''          ,
        MEM_PER_NODE           : 0           ,
        CORES_PER_NODE         : 0           ,
        GPUS_PER_NODE          : 0           ,
        SYSTEM_ARCHITECTURE    : {}          ,

        FAKE_RESOURCES         : False       ,
        MANDATORY_ARGS         : []          ,
        FORWARD_TUNNEL_ENDPOINT: ''          ,
        NEW_SESSION_PER_TASK   : True        ,
        TASK_PRE_EXEC          : []          ,
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, node_resource=None, from_dict=None):

        if not from_dict:
            from_dict = dict()

        nr = node_resource
        if nr:
            nd = nr.get('description', {})

            import pprint
            print('- NR ----------------')
            pprint.pprint(nr)
            print('=====================')

            # FIXME: no description in resource
            if not nd:
                nd = {self.N_CORES   : len(nr[NodeResources.CORE_MAP]),
                      self.N_GPUS    : len(nr[NodeResources.GPU_MAP]),
                      self.LSF       : nr[NodeResources.LSF_FREE],
                      self.MEM       : nr[NodeResources.MEM_FREE],
                      self.NODE_IDX  : None,
                      self.NODE_NAME : None,
                  }



            import pprint
            print('- ND ----------------')
            pprint.pprint(nd)
            print('=====================')

            super().__init__(from_dict=from_dict)

        self._verify


# ------------------------------------------------------------------------------
#
class NodeDescription(ru.TypedDict):

    N_CORES   = 'n_cores'
    N_GPUS    = 'n_gpus'
    LSF       = 'lsf'
    MEM       = 'mem'
    NODE_IDX  = 'node_idx'
    NODE_NAME = 'node_name'

    _schema = {
        N_CORES   : int,
        N_GPUS    : int,
        LSF       : int,
        MEM       : int,
        NODE_IDX  : str,
        NODE_NAME : str,
    }

    _defaults = {
        N_CORES   : 0,
        N_GPUS    : 0,
        LSF       : 0,
        MEM       : 0,
        NODE_IDX  : None,
        NODE_NAME : None,
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, node_resource=None, from_dict=None):

        if not from_dict:
            from_dict = dict()

        nr = node_resource
        if nr:
            nd = nr.get('description', {})

            import pprint
            print('- NR ----------------')
            pprint.pprint(nr)
            print('=====================')

            # FIXME: no description in resource
            if not nd:
                nd = {self.N_CORES   : len(nr[NodeResources.CORE_MAP]),
                      self.N_GPUS    : len(nr[NodeResources.GPU_MAP]),
                      self.LSF       : nr[NodeResources.LSF_FREE],
                      self.MEM       : nr[NodeResources.MEM_FREE],
                      self.NODE_IDX  : None,
                      self.NODE_NAME : None,
                  }



            import pprint
            print('- ND ----------------')
            pprint.pprint(nd)
            print('=====================')

            super().__init__(from_dict=from_dict)

        self._verify


# ------------------------------------------------------------------------------
#
class NodeResources(ru.TypedDict):

    DESCRIPTION = 'description'
    CORE_MAP    = 'core_map'
    GPU_MAP     = 'gpu_map'
    LSF_FREE    = 'lsf_free'
    MEM_FREE    = 'mem_free'

    _schema = {
        DESCRIPTION: NodeDescription,
        CORE_MAP   : [bool],
        GPU_MAP    : [bool],
        LSF_FREE   : int,
        MEM_FREE   : int,
    }

    _defaults = {
        DESCRIPTION: NodeDescription(),
        CORE_MAP   : [False],
        GPU_MAP    : [],
        LSF_FREE   : 0,
        MEM_FREE   : 0,
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, description=None, from_dict=None):

        if not from_dict:
            from_dict = dict()

        if description:
            from_dict['description'] = description

        if not from_dict.get('description'):
            raise ValueError('NodeResources need NodeDescription to initialize')

        print('init 1', from_dict)

        super().__init__(from_dict=from_dict)

        print('init 2', self.as_dict())

        self._verify

        print('init 3', self.as_dict())

        if not self.core_map:
            self.core_map = [False] * self.description.n_cores

        if not self.gpu_map:
            self.gpu_map = [False] * self.description.n_gpus

        if not self.lsf_free:
            self.lsf_free = self.description.lsf

        if not self.lsf_mem:
            self.lsf_mem = self.description.mem

        print('init 4', self.as_dict())


# ------------------------------------------------------------------------------

