
# ------------------------------------------------------------------------------
#
# global constants
#
MASTER                         = 'master'
WORKER                         = 'worker'

UPDATE_WORKER                  = 'update'

STAGER_WORKER                  = 'stager'
STAGER_REQUEST_QUEUE           = 'stager_request_queue'
STAGER_RESPONSE_PUBSUB         = 'stager_response_pubsub'

PMGR_LAUNCHING_QUEUE           = 'pmgr_launching_queue'
PMGR_LAUNCHING_COMPONENT       = 'pmgr_launching'

TMGR_SCHEDULING_QUEUE          = 'tmgr_scheduling_queue'
TMGR_STAGING_INPUT_QUEUE       = 'tmgr_staging_input_queue'
TMGR_STAGING_OUTPUT_QUEUE      = 'tmgr_staging_output_queue'

TMGR_SCHEDULING_COMPONENT      = 'tmgr_scheduling'
TMGR_STAGING_INPUT_COMPONENT   = 'tmgr_staging_input'
TMGR_STAGING_OUTPUT_COMPONENT  = 'tmgr_staging_output'

AGENT_STAGING_INPUT_QUEUE      = 'agent_staging_input_queue'
AGENT_SCHEDULING_QUEUE         = 'agent_scheduling_queue'
AGENT_EXECUTING_QUEUE          = 'agent_executing_queue'
AGENT_STAGING_OUTPUT_QUEUE     = 'agent_staging_output_queue'

AGENT_STAGING_INPUT_COMPONENT  = 'agent_staging_input'
AGENT_SCHEDULING_COMPONENT     = 'agent_scheduling'
AGENT_EXECUTING_COMPONENT      = 'agent_executing'
AGENT_STAGING_OUTPUT_COMPONENT = 'agent_staging_output'

TMGR_UNSCHEDULE_PUBSUB         = 'tmgr_unschedule_pubsub'
TMGR_RESCHEDULE_PUBSUB         = 'tmgr_reschedule_pubsub'

AGENT_UNSCHEDULE_PUBSUB        = 'agent_unschedule_pubsub'
AGENT_SCHEDULE_PUBSUB          = 'agent_schedule_pubsub'

CONTROL_PUBSUB                 = 'control_pubsub'
STATE_PUBSUB                   = 'state_pubsub'
LOG_PUBSUB                     = 'log_pubsub'


# ------------------------------------------------------------------------------
#
# two-state for resource occupation.
#
FREE = 0
BUSY = 1
DOWN = 2


# -----------------------------------------------------------------------------
#
# definitions of metrics
#
TASK_STATE           = 'TASK_STATE'
WAIT_QUEUE_SIZE      = 'WAIT_QUEUE_SIZE'
TMGR_METRICS         = [TASK_STATE,
                        WAIT_QUEUE_SIZE]

PILOT_STATE          = 'PILOT_STATE'
PMGR_METRICS         = [PILOT_STATE]


# -----------------------------------------------------------------------------
#
# config related
#
RESOURCE_CONFIG_LABEL_DEFAULT = 'user.cfg'


# ------------------------------------------------------------------------------
#
# staging defines
#
COPY     = 'Copy'      # local cp
LINK     = 'Link'      # local ln -s
MOVE     = 'Move'      # local mv
TRANSFER = 'Transfer'  # saga remote transfer  TODO: special case of copy?
TARBALL  = 'Tarball'   # remote staging will be executed using a tarball.

#
# Flags - inherit from RS where possible, add custom ones
#
import radical.saga.filesystem as _rsf

CREATE_PARENTS = _rsf.CREATE_PARENTS  # Create parent directories if needed
NON_FATAL      = 8192                # Don't fail the Task if input is missing


#
# Task MPI flags
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
DEFAULT_FLAGS    = CREATE_PARENTS


# scheduler names (and backwards compat)
SCHEDULER_ROUND_ROBIN  = "round_robin"
SCHEDULER_BACKFILLING  = "backfilling"
SCHEDULER_DEFAULT      = SCHEDULER_ROUND_ROBIN

# backward compatibility
UMGR_SCHEDULING_QUEUE         = TMGR_SCHEDULING_QUEUE
UMGR_STAGING_INPUT_QUEUE      = TMGR_STAGING_INPUT_QUEUE
UMGR_STAGING_OUTPUT_QUEUE     = TMGR_STAGING_OUTPUT_QUEUE

UMGR_SCHEDULING_COMPONENT     = TMGR_SCHEDULING_COMPONENT
UMGR_STAGING_INPUT_COMPONENT  = TMGR_STAGING_INPUT_COMPONENT
UMGR_STAGING_OUTPUT_COMPONENT = TMGR_STAGING_OUTPUT_COMPONENT

UMGR_UNSCHEDULE_PUBSUB        = TMGR_UNSCHEDULE_PUBSUB
UMGR_RESCHEDULE_PUBSUB        = TMGR_RESCHEDULE_PUBSUB

UMGR_METRICS                  = TMGR_METRICS

# ------------------------------------------------------------------------------

