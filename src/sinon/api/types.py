"""
.. module:: sinon.types
   :platform: Unix
   :synopsis: Type constants.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

# ------------------------------------------------------------------------------
# Pilot types
PILOT_DATA              = 1
PILOT_COMPUTE           = 2
PILOT_ANY               = PILOT_DATA | PILOT_COMPUTE

# ------------------------------------------------------------------------------
# Unit Pilot types
UNKNOWN           = -1
DATA              = 1
COMPUTE           = 2
ANY               = DATA | COMPUTE

