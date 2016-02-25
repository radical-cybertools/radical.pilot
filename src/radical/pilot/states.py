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
# common states
NEW                          = 'NEW'
DONE                         = 'DONE'
FAILED                       = 'FAILED'
CANCELED                     = 'CANCELED'

# shortcut
FINAL                        = [DONE, FAILED, CANCELED]

# pilot states
PMGR_LAUNCHING_PENDING       = 'LAUNCHING_PENDING'
PMGR_LAUNCHING               = 'LAUNCHING'
PMGR_ACTIVE_PENDING          = 'ACTIVE_PENDING'
PMGR_ACTIVE                  = 'ACTIVE'

# unit states
UMGR_SCHEDULING_PENDING      = 'UMGR_SCHEDULING_PENDING'
UMGR_SCHEDULING              = 'UMGR_SCHEDULING'
UMGR_STAGING_INPUT_PENDING   = 'UMGR_STAGING_INPUT_PENDING'
UMGR_STAGING_INPUT           = 'UMGR_STAGING_INPUT'
AGENT_STAGING_INPUT_PENDING  = 'AGENT_STAGING_INPUT_PENDING'
AGENT_STAGING_INPUT          = 'AGENT_STAGING_INPUT'
AGENT_SCHEDULING_PENDING     = 'AGENT_SCHEDULING_PENDING'
AGENT_SCHEDULING             = 'AGENT_SCHEDULING'
AGENT_EXECUTING_PENDING      = 'AGENT_EXECUTING_PENDING'
AGENT_EXECUTING              = 'AGENT_EXECUTING'
AGENT_STAGING_OUTPUT_PENDING = 'AGENT_STAGING_OUTPUT_PENDING'
AGENT_STAGING_OUTPUT         = 'AGENT_STAGING_OUTPUT'
UMGR_STAGING_OUTPUT_PENDING  = 'UMGR_STAGING_OUTPUT_PENDING'
UMGR_STAGING_OUTPUT          = 'UMGR_STAGING_OUTPUT'


# -----------------------------------------------------------------------------
# backward compatibility
#
# pilot states
CANCELING                    = CANCELED
PENDING                      = PMGR_LAUNCHING_PENDING
PENDING_LAUNCH               = PMGR_LAUNCHING_PENDING
LAUNCHING                    = PMGR_LAUNCHING
PENDING_ACTIVE               = PMGR_ACTIVE_PENDING
ACTIVE                       = PMGR_ACTIVE

# compute unit states
UNSCHEDULED                  = UMGR_SCHEDULING_PENDING
SCHEDULING                   = UMGR_SCHEDULING
PENDING_INPUT_STAGING        = UMGR_STAGING_INPUT_PENDING
STAGING_INPUT                = UMGR_STAGING_INPUT
ALLOCATING_PENDING           = AGENT_SCHEDULING_PENDING
ALLOCATING                   = AGENT_SCHEDULING
PENDING_EXECUTION            = AGENT_EXECUTING_PENDING
EXECUTING_PENDING            = AGENT_EXECUTING_PENDING
EXECUTING                    = AGENT_EXECUTING
PENDING_OUTPUT_STAGING       = UMGR_STAGING_OUTPUT_PENDING
STAGING_OUTPUT               = UMGR_STAGING_OUTPUT

# -----------------------------------------------------------------------------

