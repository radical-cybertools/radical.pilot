
# ------------------------------------------------------------------------------
#
# global constants
#
UMGR_SCHEDULING_QUEUE          = 'umgr_scheduling_queue'
UMGR_STAGING_INPUT_QUEUE       = 'umgr_staging_input_queue'
UMGR_STAGING_OUTPUT_QUEUE      = 'umgr_staging_output_queue'

UMGR_SCHEDULING_COMPONENT      = 'UmgrSchedulingComponent'
UMGR_STAGING_INPUT_COMPONENT   = 'UmgrStagingInputComponent'
UMGR_STAGING_OUTPUT_COMPONENT  = 'UmgrStagingOutputComponent'
UMGR_UPDATE_WORKER             = 'UmgrUpdateWorker'

AGENT_STAGING_INPUT_QUEUE      = 'agent_staging_input_queue'
AGENT_SCHEDULING_QUEUE         = 'agent_scheduling_queue'
AGENT_EXECUTING_QUEUE          = 'agent_executing_queue'
AGENT_STAGING_OUTPUT_QUEUE     = 'agent_staging_output_queue'

AGENT_STAGING_INPUT_COMPONENT  = 'AgentStagingInputComponent'
AGENT_SCHEDULING_COMPONENT     = 'AgentSchedulingComponent'
AGENT_EXECUTING_COMPONENT      = 'AgentExecutingComponent'
AGENT_STAGING_OUTPUT_COMPONENT = 'AgentStagingOutputComponent'
AGENT_UPDATE_WORKER            = 'AgentUpdateWorker'
AGENT_HEARTBEAT_WORKER         = 'AgentHeartbeatWorker'

PMGR_LAUNCHING_QUEUE           = 'pmgr_launching_queue'

PMGR_LAUNCHING_COMPONENT       = 'PmgrLaunchingComponent'

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


# ------------------------------------------------------------------------------
#
# staging defines
#
COPY     = 'Copy'     # local cp
LINK     = 'Link'     # local ln -s
MOVE     = 'Move'     # local mv
TRANSFER = 'Transfer' # saga remote transfer TODO: This might just be a special case of copy

#
# Flags
#
CREATE_PARENTS = 'CreateParents'  # Create parent directories while writing file
SKIP_FAILED    = 'SkipFailed'     # Don't stage out files if tasks failed

#
# Defaults
#
DEFAULT_ACTION   = TRANSFER
DEFAULT_PRIORITY = 0
DEFAULT_FLAGS    = [CREATE_PARENTS, SKIP_FAILED]
STAGING_AREA     = 'staging_area'

