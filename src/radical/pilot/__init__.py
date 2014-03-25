#pylint: disable=C0301, C0103, W0212, W0401

"""
.. module:: pilot
   :platform: Unix
   :synopsis: RADICAL-Pilot is a distributed Pilot-Job framework.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


# ------------------------------------------------------------------------------
# Scheduler name constant
from types import *
from states import *

from scheduler import *

# ------------------------------------------------------------------------------
#
from exceptions import *
from session import Session 
from credentials import SSHCredential 

from unit_manager import UnitManager
from compute_unit import ComputeUnit
from compute_unit_description  import ComputeUnitDescription

from pilot_manager import PilotManager
from compute_pilot import ComputePilot
from compute_pilot_description import ComputePilotDescription

# ------------------------------------------------------------------------------
#
from utils.version             import version
from utils.logger              import logger
