
# ------------------------------------------------------------------------------
#
# global constants
#
UPDATE_WORKER                  = 'UpdateWorker'
HEARTBEAT_WORKER               = 'HeartbeatWorker'

UMGR_SCHEDULING_QUEUE          = 'umgr_scheduling_queue'
UMGR_STAGING_INPUT_QUEUE       = 'umgr_staging_input_queue'
UMGR_STAGING_OUTPUT_QUEUE      = 'umgr_staging_output_queue'

UMGR_SCHEDULING_COMPONENT      = 'UMGRSchedulingComponent'
UMGR_STAGING_INPUT_COMPONENT   = 'UMGRStagingInputComponent'
UMGR_STAGING_OUTPUT_COMPONENT  = 'UMGRStagingOutputComponent'
UMGR_UPDATE_WORKER             = 'UMGRUpdateWorker'

AGENT_STAGING_INPUT_QUEUE      = 'agent_staging_input_queue'
AGENT_SCHEDULING_QUEUE         = 'agent_scheduling_queue'
AGENT_EXECUTING_QUEUE          = 'agent_executing_queue'
AGENT_STAGING_OUTPUT_QUEUE     = 'agent_staging_output_queue'

AGENT_STAGING_INPUT_COMPONENT  = 'AgentStagingInputComponent'
AGENT_SCHEDULING_COMPONENT     = 'AgentSchedulingComponent'
AGENT_EXECUTING_COMPONENT      = 'AgentExecutingComponent'
AGENT_STAGING_OUTPUT_COMPONENT = 'AgentStagingOutputComponent'

PMGR_LAUNCHING_QUEUE           = 'pmgr_launching_queue'

PMGR_LAUNCHING_COMPONENT       = 'PMGRLaunchingComponent'

UMGR_STATE_PUBSUB              = 'umgr_state_pubsub'

AGENT_UNSCHEDULE_PUBSUB        = 'agent_unschedule_pubsub'
AGENT_RESCHEDULE_PUBSUB        = 'agent_reschedule_pubsub'
AGENT_COMMAND_PUBSUB           = 'agent_command_pubsub'
AGENT_STATE_PUBSUB             = 'agent_state_pubsub'


# ------------------------------------------------------------------------------
#
# protocol defines
#
COMMAND_CANCEL_PILOT        = "Cancel_Pilot"
COMMAND_CANCEL_COMPUTE_UNIT = "Cancel_Compute_Unit"
COMMAND_KEEP_ALIVE          = "Keep_Alive"
COMMAND_FIELD               = "commands"
COMMAND_TYPE                = "type"
COMMAND_ARG                 = "arg"
COMMAND_CANCEL              = "Cancel"
COMMAND_SCHEDULE            = "schedule"
COMMAND_RESCHEDULE          = "reschedule"
COMMAND_UNSCHEDULE          = "unschedule"
COMMAND_WAKEUP              = "wakeup"

# two-state for resource occupation.
FREE = 'Free'
BUSY = 'Busy'


