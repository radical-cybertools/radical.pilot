#pylint: disable=C0301, C0103, W0212

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import datetime

# -----------------------------------------------------------------------------
# The state "struct" encapsulates a state and its timestamp.
class State(object):
    __slots__ = ('state', 'timestamp')

    # ----------------------------------------
    #
    def __init__(self, state, timestamp):
        """Le constructeur.
        """
        self.state = state
        self.timestamp = timestamp

    # ----------------------------------------
    #
    def __eq__(self, other):
        """Comparison operator for 'equal'. Compares 'State' as well 
           as 'str' types.
        """
        if type(other) == str:
            return self.state == other
        else:
            return self.state == other.state

    # ----------------------------------------
    #
    def __ne__(self, other):
        """Comparison operator for 'not equal'. Compares 'State' as well 
           as 'str' types.
        """
        if type(other) == str:
            return not self.__eq__(other)
        else:
            return not self.__eq__(other.state)

    # ----------------------------------------
    #
    def as_dict(self):
        """Returns the state and its timestamp as a Python dictionary.
        """
        return {"state": self.state, "timestamp": self.timestamp}

    # ----------------------------------------
    #
    def __str__(self):
        """Returns the string representation of the state. The string 
           representation is just the state as string without its timestamp.
        """
        return self.state

# -----------------------------------------------------------------------------
# Common States
NEW                          = 'New'
DONE                         = 'Done'
CANCELING                    = 'Canceling'
CANCELED                     = 'Canceled'
FAILED                       = 'Failed'
PENDING                      = 'Pending'

# -----------------------------------------------------------------------------
# ComputePilot States
PENDING_LAUNCH               = 'PendingLaunch'
LAUNCHING                    = 'Launching'
PENDING_ACTIVE               = 'PendingActive'
ACTIVE                       = 'Active'

# -----------------------------------------------------------------------------
# ComputeUnit States
UNSCHEDULED                  = 'Unscheduled'
PENDING_INPUT_STAGING        = 'PendingInputStaging'
STAGING_INPUT                = 'StagingInput'
AGENT_STAGING_INPUT_PENDING  = 'AgentStagingInputPending'
AGENT_STAGING_INPUT          = 'AgentStagingInput'
PENDING_EXECUTION            = 'PendingExecution'
SCHEDULING                   = 'Scheduling'
ALLOCATING_PENDING           = 'AllocatingPending'
ALLOCATING                   = 'Allocating'
EXECUTING_PENDING            = 'ExecutingPending'
EXECUTING                    = 'Executing'
AGENT_STAGING_OUTPUT_PENDING = 'AgentStagingOutputPending'
AGENT_STAGING_OUTPUT         = 'AgentStagingOutput'
PENDING_OUTPUT_STAGING       = 'PendingOutputStaging'
STAGING_OUTPUT               = 'StagingOutput'

