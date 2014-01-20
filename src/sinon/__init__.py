"""
.. module:: sinon
   :platform: Unix
   :synopsis: Sinon (a.k.a SAGA-Pilot) is a distributed Pilot-Job framework.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, radical.rutgers.edu"
__license__   = "MIT"

# ------------------------------------------------------------------------------
#
import sinon.types as types
import sinon.states as states


# ------------------------------------------------------------------------------
#
from sinon.session                   import Session 
from sinon.credentials               import SSHCredential 
from sinon.exceptions                import SinonException

from sinon.unit_manager              import UnitManager
from sinon.compute_unit_description  import ComputeUnitDescription

from sinon.pilot_manager             import PilotManager
from sinon.compute_pilot             import ComputePilot
from sinon.compute_pilot_description import ComputePilotDescription

# ------------------------------------------------------------------------------
#
from sinon.utils.version                 import version
from sinon.utils.logger                  import logger

logger.info ('loading sinon version: %s' % version)



