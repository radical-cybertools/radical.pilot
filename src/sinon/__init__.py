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
from sinon.api.credential                import Context 
from sinon.api.exceptions                import SinonException

from sinon.api.unit_manager              import UnitManager
from sinon.api.compute_unit_description  import ComputeUnitDescription

from sinon.api.pilot_manager             import PilotManager
from sinon.api.compute_pilot             import ComputePilot
from sinon.api.compute_pilot_description import ComputePilotDescription


# ------------------------------------------------------------------------------
#
import os
import radical.utils.logger as rul

version=open    (os.path.dirname (os.path.abspath (__file__)) + "/VERSION", 'r').read().strip()
rul.getLogger   ('sinon').info ('sinon           version: %s' % version)
# rul.log_version ('sinon', 'sinon', version)


# ------------------------------------------------------------------------------

