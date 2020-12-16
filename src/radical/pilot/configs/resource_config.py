

import radical.utils as ru

PilotConfig :
    {
        'named_envs' : {
            str : EnvironmentConfig:
            {
                'kind'  : str,
                'script': str,
            }
        },
        'resource' : ResourceConfig:
        {
            'cfg_name'      : str,
            'description'   : str,
            'lrms'          : str,
            'cores_per_node': int,
            'gpus_per_node' : int,
            'mem_per_node'  : int,
            'lfs_per_node'  : int,
            'lfs_path'      : str,
            'access'        : AccessConfig:
            {
                'cfg_name'       : str,
                'job_manager'    : str,
                'job_manager_hop': str,
                'filesystem'     : str,

            }
        },
        'system' : SystemConfig:
        {
            'cfg_name'       :  str,
            'valid_roots'    : [str],
            'sandbox_base'   :  str,
            'python_dist'    :  str,
            'virtenv'        :  str,
            'virtenv_mode'   :  str,
            'rp_version'     :  str,
        }
        'partitions' :
        [
            PartitionConfig:
            {
                'cfg_name'       :  str,
                'named_env'      :  str,
                'bridges':
                [
                    BridgeConfig:
                    {
                        'cfg_name' : str,
                        'kind'     : str,
                        'log_lvl'  : str,
                        'stall_hwm': int,
                        'bulk_size': int,
                    }
                ]
                'agents':
                [
                    AgentConfig:
                    {
                        'cfg_name'  : str,
                        'mode'      : str,
                        'target'    : str,
                        'components':
                        [
                            ComponentConfig:
                            {
                                'count': int,
                            }
                        ]
                    }
                ]
                'launch_methods':
                [
                    LaunchMethodConfig:
                    {
                        'cfg_name'       :  str,
                        'named_env'      :  str,
                        'tags'           : [str],
                        'export_to_task' : [str],
                        'pre_exec_launch': [str],
                        'pre_exec_task'  : [str],
                        'pre_exec_rank'  : {int: [str]},
                    }
                ]
            }
        ]
    }


# ------------------------------------------------------------------------------
#
class EnvironmentConfig(ru.Config):

    _schema = {
            'kind'  : str,
            'script': str,
    }

    _defaults = {
            'kind'  : 'virtualenv',  # conda, conda-pack, module, script
            'script': '',
    }


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
class AccessConfig(ru.Config):

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
class BridgeConfig(ru.Config):

    _schema = {
            'kind'     : str,
            'log_lvl'  : str,
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
class ComponentConfig(ru.Config):

    _schema = {
            'name'     : str,
            'count'    : int,
    }

    defaults = {
            'name'     : None,
            'count'    : 1,
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
class PartitionConfig(ru.Config):

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

            'bridges'              : {str: BridgeConfig},
            'agents'               : [AgentConfig],
    }

    _defaults = {
            'bulk_collection_size' : 1024,
            'bulk_collection_time' : 1.0,
            'db_poll_sleeptime'    : 2.0,

            'bulk_time'            : 1.0,
            'bulk_size'            : 1024,

            'heartbeat_interval'   : 10.0,
            'heartbeat_timeout'    : 60.0,

            'bridges'              : {
                'agent_scheduling'    : BridgeConfig({'kind': 'queue'}),
                'agent_executing'     : BridgeConfig({'kind': 'queue'}),
                'agent_staging_input' : BridgeConfig({'kind': 'queue'})
                'agent_staging_output': BridgeConfig({'kind': 'queue'}),

                'funcs_req'           : BridgeConfig({'kind': 'queue'}),
                'funcs_res'           : BridgeConfig({'kind': 'queue'}),
                'agent_unschedule'    : BridgeConfig({'kind': 'pubsub'}),
                'agent_schedule'      : BridgeConfig({'kind': 'pubsub'}),

                'control'             : BridgeConfig({'kind': 'pubsub'}),
                'state'               : BridgeConfig({'kind': 'pubsub'}),
              # 'log_pubsub'          : BridgeConfig({'kind': 'pubsub'}),
            },

            'agents'               : [AgentConfig()],

    }


# ------------------------------------------------------------------------------
#
class AgentConfig(ru.Config):

    _schema = {
            'target'    : str,
            'mode'      : str,
            'components': [ComponentConfig],
    }

    _default = {
            'target'    : 'local',
            'mode'      : 'shared',
            'components': [
                ComponentConfig({'name' : 'update',
                                 'count': 1}),
                ComponentConfig({'name' : 'agent_staging_input',
                                 'count': 1}),
                ComponentConfig({'name' : 'agent_scheduling',
                                 'count': 1}),
                ComponentConfig({'name' : 'agent_executing',
                                 'count': 1}),
                ComponentConfig({'name' : 'agent_staging_output',
                                 'count': 1}),
            ],
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

