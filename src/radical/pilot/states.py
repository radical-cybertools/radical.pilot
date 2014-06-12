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
            return not self.__eq(other.state)

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
NEW                         = 'New'
NULL                        = 'Null'
PENDING                     = 'Pending'
DONE                        = 'Done'
CANCELED                    = 'Canceled'
FAILED                      = 'Failed'

# -----------------------------------------------------------------------------
# ComputePilot States
PENDING_LAUNCH              = 'PendingLaunch'
LAUNCHING                   = 'Launching'
PENDING_ACTIVE              = 'PendingActive'
ACTIVE                      = 'Active'

# -----------------------------------------------------------------------------
# ComputeUnit States
PENDING_EXECUTION           = 'PendingExecution'
SCHEDULING                  = 'Scheduling'
EXECUTING                   = 'Executing'

PENDING_INPUT_STAGING       = 'PendingInputStaging'  # These last 4 are not really states,
STAGING_INPUT               = 'StagingInput'         # as there are distributed entities enacting on them.
PENDING_OUTPUT_STAGING      = 'PendingOutputStaging' # They should probably just go,
STAGING_OUTPUT              = 'StagingOutput'        # and be turned into logging events.
