"""
.. module:: sagapilot
   :platform: Unix
   :synopsis: SAGA-Pilot (a.k.a SAGA-Pilot) is a distributed Pilot-Job framework.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

# ------------------------------------------------------------------------------
#
import sagapilot.types as types
import sagapilot.states as states

# ------------------------------------------------------------------------------
# Scheduler name constant
from sagapilot.plugins                   import *

# ------------------------------------------------------------------------------
#
from sagapilot.session                   import Session 
from sagapilot.credentials               import SSHCredential 
from sagapilot.exceptions                import *

from sagapilot.unit_manager              import UnitManager
from sagapilot.compute_unit              import ComputeUnit
from sagapilot.compute_unit_description  import ComputeUnitDescription

from sagapilot.pilot_manager             import PilotManager
from sagapilot.compute_pilot             import ComputePilot
from sagapilot.compute_pilot_description import ComputePilotDescription

# ------------------------------------------------------------------------------
#
from sagapilot.utils.version             import version
from sagapilot.utils.logger              import logger


logger.info ('loading SAGA-Pilot version: %s' % version)



