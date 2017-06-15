
# ------------------------------------------------------------------------------
#
# global constants
#
UPDATE_WORKER                  = 'UpdateWorker'

PMGR_LAUNCHING_QUEUE           = 'pmgr_launching_queue'
PMGR_LAUNCHING_COMPONENT       = 'PMGRLaunchingComponent'

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
# AGENT_UPDATE_QUEUE             = 'agent_update_queue'

AGENT_STAGING_INPUT_COMPONENT  = 'AgentStagingInputComponent'
AGENT_SCHEDULING_COMPONENT     = 'AgentSchedulingComponent'
AGENT_EXECUTING_COMPONENT      = 'AgentExecutingComponent'
AGENT_STAGING_OUTPUT_COMPONENT = 'AgentStagingOutputComponent'
# AGENT_UPDATE_WORKER            = 'AgentUpdateWorker'

UMGR_UNSCHEDULE_PUBSUB         = 'umgr_unschedule_pubsub'
UMGR_RESCHEDULE_PUBSUB         = 'umgr_reschedule_pubsub'

AGENT_UNSCHEDULE_PUBSUB        = 'agent_unschedule_pubsub'
AGENT_RESCHEDULE_PUBSUB        = 'agent_reschedule_pubsub'

CONTROL_PUBSUB                 = 'control_pubsub'
STATE_PUBSUB                   = 'state_pubsub'
LOG_PUBSUB                     = 'log_pubsub'


# ------------------------------------------------------------------------------
#
# two-state for resource occupation.
#
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
NON_FATAL      = 'NonFatal'       # Don't fail the CU if input is missing

#
# CU MPI flags
#
SERIAL         = 'Serial'
MPI            = 'MPI'
OpenMP         = 'OpenMP'
GPU            = 'GPU'
GPU_MPI        = 'GPU_MPI'
GPU_OpenMP     = 'GPU_OpenMP'

#
# Defaults
#
DEFAULT_ACTION   = TRANSFER
DEFAULT_PRIORITY = 0
DEFAULT_FLAGS    = [CREATE_PARENTS, SKIP_FAILED]
STAGING_AREA     = 'staging_area'


# scheduler names (and backwards compat)
SCHEDULER_ROUND_ROBIN  = "round_robin"
SCHEDULER_BACKFILLING  = "backfilling"
SCHEDULER_DEFAULT      = SCHEDULER_ROUND_ROBIN

