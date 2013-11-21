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
from sinon.version    import VERSION as version

# ------------------------------------------------------------------------------
#
import sinon.frontend.types as types
import sinon.frontend.states as states

# ------------------------------------------------------------------------------
#
from sinon.frontend.session                   import Session 
from sinon.frontend.exceptions                import SinonException

from sinon.frontend.unit_manager              import UnitManager
from sinon.frontend.compute_unit_description  import ComputeUnitDescription

from sinon.frontend.pilot_manager             import PilotManager
from sinon.frontend.compute_pilot             import ComputePilot
from sinon.frontend.compute_pilot_description import ComputePilotDescription
