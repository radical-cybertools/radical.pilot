#pylint: disable=C0301, C0103, W0212

"""
.. module:: sagapilot.states
   :platform: Unix
   :synopsis: State constants.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

# ------------------------------------------------------------------------------
# States
UNKNOWN             = 'Unknown'
PENDING             = 'Pending'
TRANSFERRING_INPUT  = 'TransferringInput'
RUNNING             = 'Running'
TRANSFERRING_OUTPUT = 'TransferringOutput' 
DONE                = 'Done'
CANCELED            = 'Canceled'
FAILED              = 'Failed' 
