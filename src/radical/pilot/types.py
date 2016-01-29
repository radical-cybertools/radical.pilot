
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

# ------------------------------------------------------------------------------
# Pilot types
PILOT_DATA        = 1
PILOT_COMPUTE     = 2
PILOT_ANY         = PILOT_DATA | PILOT_COMPUTE

# ------------------------------------------------------------------------------
# Unit Pilot types
UNKNOWN           = -1
DATA              = 1
COMPUTE           = 2
ANY               = DATA | COMPUTE

# -----------------------------------------------------------------------------
#
# definitions of metrics
#
UNIT_STATE           = 'UNIT_STATE'
WAIT_QUEUE_SIZE      = 'WAIT_QUEUE_SIZE'
UMGR_METRICS         = [UNIT_STATE, 
                        WAIT_QUEUE_SIZE]

PILOT_STATE          = 'PILOT_STATE'
PMGR_METRICS         = [PILOT_STATE]



