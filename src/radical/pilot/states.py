#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.states
   :platform: Unix
   :synopsis: State constants.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"


# -----------------------------------------------------------------------------
# States
UNKNOWN                 = 'Unknown'
NEW                     = 'New'

PENDING                 = 'Pending'

PENDING_EXECUTION       = 'PendingExecution'
RUNNING                 = 'Running'

PENDING_INPUT_TRANSFER  = 'PendingInputTransfer'
TRANSFERRING_INPUT      = 'TransferringInput'

PENDING_OUTPUT_TRANSFER = 'PendingOutputTransfer'
TRANSFERRING_OUTPUT     = 'TransferringOutput'

DONE                    = 'Done'
CANCELED                = 'Canceled'
FAILED                  = 'Failed'
