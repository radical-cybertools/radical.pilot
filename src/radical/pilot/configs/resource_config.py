

import radical.utils as ru

PilotConfig :
    {
        ResourceConfig :
        {
            'description'    : str,
            'lrms'           : str,

            'cores_per_node' : int,
            'gpus_per_node'  : int,
            'mem_per_node'   : int,
            'lfs_per_node'   : int,
            'lfs_path'       : str,
        },
        AccessConfig:
        {
            'default'        : str,
            'options'        :
            {
                str: AccessConfigOption :
                {
                    'job_manager'    : str,
                    'job_manager_hop': str,
                    'filesystem'     : str,
                }
            }
        }
        SystemConfig:
        {
            'valid_roots'    : [str],
            'sandbox_base'   :  str,
            'python_dist'    :  str,
            'virtenv'        :  str,
            'virtenv_mode'   :  str,
            'rp_version'     :  str,
            'pre_bootstrap_0': [str],
            'pre_bootstrap_1': [str],
            'pre_bootstrap_2': [str],
            'export_to_task'   : [str],
            'pre_exec_task'    : [str],
            'options'        :
            {
                str: SystemConfigOption :
                {
                   'pre_bootstrap_0': [str],
                   'pre_bootstrap_1': [str],
                   'pre_bootstrap_2': [str],
                   'export_to_task'   : [str],
                   'pre_exec_task'    : [str],
                }
            }
        }
        AgentConfig:
        [
            PartitionConfig:
        ]


# ------------------------------------------------------------------------------
#
class ResourceConfig(ru.Config):

    _schema = {
            'description'    : str,
            'lrms'           : str,

            'cores_per_node' : int,
            'gpus_per_node'  : int,
            'mem_per_node'   : int,
            'lfs_per_node'   : int,
            'lfs_path'       : str,
    }

    _defaults = {
            'description'    : '',
            'lrms'           : 'FORK',

            'cores_per_node' : 1,
            'gpus_per_node'  : 0,
            'mem_per_node'   : 0,
            'lfs_per_node'   : 0,
            'lfs_path'       : '/tmp/',
    }


# ------------------------------------------------------------------------------
#
class AccessConfigOption(ru.Config):

    _schema = {
            'job_manager'    : str,
            'job_manager_hop': str,
            'filesystem'     : str,
    }

    _defaults = {
            'job_manager'    : 'fork://localhost/',
            'job_manager_hop': None,
            'filesystem'     : 'file://localhost/',
    }


# ------------------------------------------------------------------------------
#
class AccessConfig(ru.Config):

    _schema = {
            'default'        : str,
            'options'        : {str: AccessConfigOption}
    }

    _defaults = {
            'default'        : 'fork',
            'options'        : {'fork': AccessConfigOption()}
    }


# ------------------------------------------------------------------------------
#
class SystemConfig(ru.Config):

    _schema = {
            'valid_roots'    : [str],
            'sandbox_base'   :  str,
            'python_dist'    :  str,
            'virtenv'        :  str,
            'virtenv_mode'   :  str,
            'rp_version'     :  str,
            'pre_bootstrap_0': [str],
            'pre_bootstrap_1': [str],
            'pre_bootstrap_2': [str],
            'export_to_task'   : [str],
            'pre_exec_task'    : [str],
    }


    _defaults = {
            'valid_roots'    : ['/'],
            'sandbox_base'   : '$HOME',
            'python_dist'    : 'default',
            'virtenv'        : None,
            'virtenv_mode'   : 'create',
            'rp_version'     : 'local',
            'pre_bootstrap_0': None,
            'pre_bootstrap_1': None,
            'pre_bootstrap_2': None,
            'export_to_task'   : None,
            'pre_exec_task'    : None,
    }


# ------------------------------------------------------------------------------
#
class BridgeOption(ru.Config):

    _schema = {
            'kind'     : str,
            'log_lvl'  : str
            'stall_hwm': int,
            'bulk_size': int,
    }

    defaults = {
            'log_lvl'  : 'error',
            'stall_hwm': 0,
            'bulk_size': 0,
    }


# ------------------------------------------------------------------------------
#
class ComponentOption(ru.Config):

    _schema = {
            'count'    : int,
    }

    defaults = {
            'count'    : 1,
    }


# ------------------------------------------------------------------------------
#
class SubAgentConfig(ru.Config):

    _schema = {
            'components': {str: ComponentOption},
    }

    _default = {
            'components': {
                'update'               : ComponentOption({'count' : 1}),
                'agent_staging_input'  : ComponentOption({'count' : 1}),
                'agent_scheduling'     : ComponentOption({'count' : 1}),
                'agent_executing'      : ComponentOption({'count' : 1}),
                'agent_staging_output' : ComponentOption({'count' : 1}),
            },
    }


# ------------------------------------------------------------------------------
#
class LaunchMethodConfig(ru.Config):

    _schema = {
            'tags'           : [str],
            'pre_bootstrap_0': [str],
            'pre_bootstrap_1': [str],
            'pre_bootstrap_2': [str],
            'export_to_task' : [str],
            'pre_exec_launch': [str],
            'pre_exec_task'  : [str],
            'pre_exec_rank'  : {int: [str]},
    }

    _defaults = {
            'tags'           : [],
            'pre_bootstrap_0': [],
            'pre_bootstrap_1': [],
            'pre_bootstrap_2': [],
            'export_to_task' : [],
            'pre_exec_launch': [],
            'pre_exec_task'  : [],
            'pre_exec_rank'  : {},
    }



# ------------------------------------------------------------------------------
#
class AgentConfig(ru.Config):

    _schema = {
            'pre_bootstrap_0': [str],
            'pre_bootstrap_1': [str],
            'pre_bootstrap_2': [str],
            'export_to_task'   : [str],
            'pre_exec_task'    : [str],
            'launch_methods' : {str: LaunchMethodConfig}
    }

    _defaults = {
            'pre_bootstrap_0': None,
            'pre_bootstrap_1': None,
            'pre_bootstrap_2': None,
            'export_to_task'   : None,
            'pre_exec_task'    : None,
    }


# ------------------------------------------------------------------------------
#
class PartitionConfigOption(ru.Config):

    _schema = {
            'bulk_collection_size' : int,
            'bulk_collection_time' : float,
            'db_poll_sleeptime'    : float,

            'target'               : str,
            'mode'                 : str,

            'bulk_time'            : float,
            'bulk_size'            : int,

            'heartbeat_interval'   : float,
            'heartbeat_timeout'    : float,

            'bridges'              : {str: BridgeOption},
            'sub_agents'           : [SubAgentConfig],
    }

    _defaults = {
            'bulk_collection_size' : 1024,
            'bulk_collection_time' : 1.0,
            'db_poll_sleeptime'    : 2.0,

            'target'               : 'local',
            'mode'                 : 'shared',

            'bulk_time'            : 1.0,
            'bulk_size'            : 1024,

            'heartbeat_interval'   : 10.0,
            'heartbeat_timeout'    : 60.0,

            'bridges'              : {
                'agent_scheduling'    : BridgeOption({'kind': 'queue'}),
                'agent_executing'     : BridgeOption({'kind': 'queue'}),
                'agent_staging_input' : BridgeOption({'kind': 'queue'})
                'agent_staging_output': BridgeOption({'kind': 'queue'}),

                'funcs_req'           : BridgeOption({'kind': 'queue'}),
                'funcs_res'           : BridgeOption({'kind': 'queue'}),
                'agent_unschedule'    : BridgeOption({'kind': 'pubsub'}),
                'agent_schedule'      : BridgeOption({'kind': 'pubsub'}),

                'control'             : BridgeOption({'kind': 'pubsub'}),
                'state'               : BridgeOption({'kind': 'pubsub'}),
              # 'log_pubsub'          : BridgeOption({'kind': 'pubsub'}),
            },

            'sub_agents'           : [SubAgentConfig()],

    }


# ------------------------------------------------------------------------------
#
class PartitionConfig(ru.Config):

    _schema = {
            'default'        : str,
            'options'        : {str: PartitionConfigOption},
    }

    _defaults = {
            'default'        : 'single',
            'options'        : {'single': PartitionConfigOption()},
    }


# ------------------------------------------------------------------------------
#
class AgentConfig(ru.Config):

    _schema = {
            'partitions'     : [PartitionConfig],
    }


    _defaults = {
            'partitions'     : [PartitionConfig()],
    }

# ------------------------------------------------------------------------------
#
class PilotConfig(ru.Config):

    _schema = {
            'resource'       : ResourceConfig,
            'access'         : AccessConfig,
            'system'         : SystemConfig,
            'partitions'     : [PartitionConfig],
    }

    _defaults = {
            'resource'       : ResourceConfig(),
            'access'         : AccessConfig(),
            'system'         : SystemConfig(),
            'agent'          : AgentConfig(),
    }


# ------------------------------------------------------------------------------

