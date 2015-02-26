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
from types     import *
from states    import *
from logentry  import * 
from scheduler import *

# ------------------------------------------------------------------------------
#
from url import Url
from exceptions import *
from session import Session 
from context import Context

from unit_manager import UnitManager
from compute_unit import ComputeUnit
from compute_unit_description  import ComputeUnitDescription

from pilot_manager import PilotManager
from compute_pilot import ComputePilot
from compute_pilot_description import ComputePilotDescription

from resource_config import ResourceConfig

from staging_directives import COPY, LINK, MOVE, TRANSFER, SKIP_FAILED, CREATE_PARENTS

# ------------------------------------------------------------------------------
#
from utils.logger              import logger

import os

import radical.utils        as ru
import radical.utils.logger as rul


pwd     = os.path.dirname (__file__)
root    = "%s/.." % pwd
version, version_detail, version_branch, sdist_name, sdist_path = ru.get_version ([root, pwd])

# FIXME: the logger init will require a 'classical' ini based config, which is
# different from the json based config we use now.   May need updating once the
# radical configuration system has changed to json
_logger = rul.logger.getLogger  ('radical.pilot')
_logger.info ('radical.pilot        version: %s' % version_detail)

# ------------------------------------------------------------------------------

