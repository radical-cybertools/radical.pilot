# pylint: disable=access-member-before-definition

__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

DESCRIPTION            = 'description'
NOTES                  = 'notes'
SCHEMAS                = 'schemas'
DEFAULT_REMOTE_WORKDIR = 'default_remote_workdir'
DEFAULT_QUEUE          = 'default_queue'
RESOURCE_MANAGER       = 'default_manager'
AGENT_CONFIG           = 'agent_config'
AGENT_SCHEDULER        = 'agent_scheduler'
AGENT_SPAWNER          = 'agent_spawner'
PRE_BOOTSTRAP_0        = 'pre_bootstrap_0'
PRE_BOOTSTRAP_1        = 'pre_bootstrap_1'
RP_VERSION             = 'rp_version'
VIRTENV_MODE           = 'virtenv_mode'
VIRTENV_DIST           = 'virtenv_dist'
PYTHON_DIST            = 'python_dist'
LAUNCH_METHODS         = 'launch_methods'
LFS_PATH_PER_NODE      = 'lfs_path_per_node'
LFS_SIZE_PER_NODE      = 'lfs_size_per_node'
MEM_PER_NODE           = 'mem_per_node'
CORES_PER_NODE         = 'cores_per_node'
GPUS_PER_NODE          = 'gpus_per_node'
BLOCKED_CORES          = 'blocked_cores'
BLOCKED_GPUS           = 'blocker_gpus'
SYSTEM_ARCHITECTURE    = 'system_architecture'

# ------------------------------------------------------------------------------
#
class ResourceDescription(ru.TypedDict):
    """
    docstrings goes here
    """

    _schema = {
        DESCRIPTION            : str  ,
        NOTES                  : str   ,
        SCHEMAS                : [dict()] ,

        DEFAULT_REMOTE_WORKDIR : str   ,
        DEFAULT_QUEUE          : str   ,
        RESOURCE_MANAGER       : str   ,
        AGENT_CONFIG           : str   ,
        AGENT_SCHEDULER        : str   ,
        AGENT_SPAWNER          : str   ,
        PRE_BOOTSTRAP_0        : [str] ,
        PRE_BOOTSTRAP_1        : [str] ,
        RP_VERSION             : str   ,
        VIRTENV_MODE           : str   ,
        VIRTENV_DIST           : str   ,
        PYTHON_DIST            : str   ,
        LAUNCH_METHODS         : dict(),
        LFS_PATH_PER_NODE      : str   ,
        LFS_SIZE_PER_NODE      : str   ,
        MEM_PER_NODE           : int   ,
        CORES_PER_NODE         : int   ,
        GPUS_PER_NODE          : int   ,
        BLOCKED_CORES          : [int] ,
        BLOCKED_GPUS           : [int] ,
        SYSTEM_ARCHITECTURE    : dict(),
    }

    _defaults = {
        DESCRIPTION            : ''          ,
        NOTES                  : ''          ,
        SCHEMAS                : [dict()]    ,

        DEFAULT_REMOTE_WORKDIR : ''          ,
        DEFAULT_QUEUE          : ''          ,
        RESOURCE_MANAGER       : ''          ,
        AGENT_CONFIG           : 'default'   ,
        AGENT_SCHEDULER        : 'CONTINUOUS',
        AGENT_SPAWNER          : 'POPEN'     ,
        PRE_BOOTSTRAP_0        : list()      ,
        PRE_BOOTSTRAP_1        : list()      ,
        RP_VERSION             : ''          ,
        VIRTENV_MODE           : ''          ,
        VIRTENV_DIST           : ''          ,
        PYTHON_DIST            : 'default'   ,
        LAUNCH_METHODS         : dict()      ,
        LFS_PATH_PER_NODE      : str         ,
        LFS_SIZE_PER_NODE      : str         ,
        MEM_PER_NODE           : int         ,
        CORES_PER_NODE         : int         ,
        GPUS_PER_NODE          : int         ,
        BLOCKED_CORES          : [int]       ,
        BLOCKED_GPUS           : [int]       ,
        SYSTEM_ARCHITECTURE    : dict()      ,
        
    }


    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):

        super().__init__(from_dict=from_dict)


    # --------------------------------------------------------------------------
    #
    def _verify(self):

        pass


# ------------------------------------------------------------------------------