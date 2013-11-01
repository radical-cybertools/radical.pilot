"""
.. module:: sinon
   :platform: Unix
   :synopsis: Sinon (a.k.a SAGA-Pilot) is a distributed Pilot-Job framework.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

# ------------------------------------------------------------------------------
#
from sinon.version import VERSION as version

# ------------------------------------------------------------------------------
#
from sinon.exceptions                  import SinonException
from sinon.session                     import Session
from sinon.pilot                       import Pilot
from sinon.pilot_manager               import PilotManager
from sinon.unit_manager                import UnitManager
from sinon.compute_unit_description    import ComputeUnitDescription
from sinon.compute_pilot_description   import ComputePilotDescription

