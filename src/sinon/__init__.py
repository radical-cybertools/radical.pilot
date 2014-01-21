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
import sinon.api.types as types
import sinon.api.states as states


# ------------------------------------------------------------------------------
#
from sinon.api.session                   import Session 
from sinon.api.credentials               import SSHCredential 
from sinon.api.exceptions                import SinonException

from sinon.api.unit_manager              import UnitManager
from sinon.api.compute_unit_description  import ComputeUnitDescription

from sinon.api.pilot_manager             import PilotManager
from sinon.api.compute_pilot             import ComputePilot
from sinon.api.compute_pilot_description import ComputePilotDescription


# ------------------------------------------------------------------------------
#
from sinon.utils.version                 import version
from sinon.utils.logger                  import logger

logger.info ('loading sinon version: %s' % version)



