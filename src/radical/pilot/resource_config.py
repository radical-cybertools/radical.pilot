
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

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
        SYSTEM_ARCHITECTURE    : dict()      ,
        SCATTERED              : False       ,

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
