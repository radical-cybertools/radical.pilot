#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.states
   :platform: Unix
   :synopsis: State constants.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

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
UNKNOWN                      = 'Unknown'
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

# forward compatible
AGENT_SCHEDULING_PENDING     = ALLOCATING_PENDING
AGENT_SCHEDULING             = ALLOCATING
AGENT_EXECUTING_PENDING      = EXECUTING_PENDING
AGENT_EXECUTING              = EXECUTING

UMGR_SCHEDULING_PENDING      = UNSCHEDULED
UMGR_SCHEDULING              = SCHEDULING
UMGR_STAGING_INPUT_PENDING   = PENDING_INPUT_STAGING
UMGR_STAGING_INPUT           = STAGING_INPUT
UMGR_STAGING_OUTPUT_PENDING  = PENDING_OUTPUT_STAGING
UMGR_STAGING_OUTPUT          = STAGING_OUTPUT

PMGR_LAUNCHING_PENDING       = PENDING_LAUNCH
PMGR_LAUNCHING               = LAUNCHING
PMGR_ACTIVE_PENDING          = PENDING_ACTIVE
PMGR_ACTIVE                  = ACTIVE


# ------------------------------------------------------------------------------
#
def derive_unit_state(hist):

    _unit_state_value = {UNKNOWN                      :  0,
                         NEW                          :  1,
                         UNSCHEDULED                  :  2,
                         PENDING_INPUT_STAGING        :  3,
                         STAGING_INPUT                :  4,
                         AGENT_STAGING_INPUT_PENDING  :  5,
                         AGENT_STAGING_INPUT          :  6,
                         PENDING_EXECUTION            :  7,
                         SCHEDULING                   :  8,
                         ALLOCATING_PENDING           :  9,
                         ALLOCATING                   : 10,
                         EXECUTING_PENDING            : 11,
                         EXECUTING                    : 12,
                         AGENT_STAGING_OUTPUT_PENDING : 13,
                         AGENT_STAGING_OUTPUT         : 14,
                         PENDING_OUTPUT_STAGING       : 15,
                         STAGING_OUTPUT               : 16,
                         DONE                         : 17,
                         FAILED                       : 18,
                         CANCELED                     : 19}

    _inv_unit_state_value = {v: k for k, v in _unit_state_value.items()}

  # state = _inv_unit_state_value(max([_unit_state_value(x['state']) for x in hist]))
    state = _unit_state_value(UNKNOWN)

    for s,t in hist:
        state = max(state, _unit_state_value[s])

    return _inv_unit_state_value(state)


# ------------------------------------------------------------------------------
#
def derive_pilot_state(hist):

    _pilot_state_value = {UNKNOWN        :  0,
                          PENDING_LAUNCH :  1,
                          LAUNCHING      :  2,
                          PENDING_ACTIVE :  3,
                          ACTIVE         :  4,
                          DONE           :  5,
                          FAILED         :  6,
                          CANCELED       :  7}

    _inv_pilot_state_value = {v: k for k, v in _pilot_state_value.items()}

    state = _pilot_state_value(UNKNOWN)

    for s,t in hist:
        state = max(state, _pilot_state_value[s])

    return _inv_pilot_state_value(state)


# ------------------------------------------------------------------------------

