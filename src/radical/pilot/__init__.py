
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


# ------------------------------------------------------------------------------
# we *first* import radical.utils, so that the monkeypatching of the logger has
# a chance to kick in before the logging module is pulled by any other 3rd party
# module, and also to monkeypatch `os.fork()` for the `atfork` functionality
import radical.utils as _ru

# ------------------------------------------------------------------------------
# constants and types
from .states     import *
from .constants  import *


# ------------------------------------------------------------------------------
# import API
from .session                   import Session
from .context                   import Context

from .unit_manager              import UnitManager
from .compute_unit              import ComputeUnit
from .compute_unit_description  import ComputeUnitDescription
from .compute_unit_description  import POSIX, MPI, OpenMP, CUDA, FUNC

from .pilot_manager             import PilotManager
from .compute_pilot             import ComputePilot
from .compute_pilot_description import ComputePilotDescription


# ------------------------------------------------------------------------------
# make submodules available -- mostly for internal use
from . import utils
from . import worker
from . import umgr
from . import pmgr
from . import agent

from .agent import Agent_0
from .agent import Agent_n


# ------------------------------------------------------------------------------
#
# get version info
#
import os as _os

version_short, version_detail, version_base, version_branch, \
        sdist_name, sdist_path = _ru.get_version(_os.path.dirname(__file__))

version = version_short


# ------------------------------------------------------------------------------

