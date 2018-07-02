
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


# ------------------------------------------------------------------------------
# we *first* import radical.utils, so that the monkeypatching of the logger has
# a chance to kick in before the logging module is pulled by any other 3rd party
# module, and also to monkeypatch `os.fork()` for the `atfork` functionality
import radical.utils as _ru

# ------------------------------------------------------------------------------
# constants and types
from .types      import *
from .states     import *
from .logentry   import * 
from .constants  import *
from .exceptions import *


# ------------------------------------------------------------------------------
# import API
from .session                   import Session
from .context                   import Context

from .unit_manager              import UnitManager
from .compute_unit              import ComputeUnit
from .compute_unit_description  import ComputeUnitDescription
from .compute_unit_description  import POSIX, MPI, OpenMP

from .pilot_manager             import PilotManager
from .compute_pilot             import ComputePilot
from .compute_pilot_description import ComputePilotDescription

from .resource_config           import ResourceConfig
from .staging_directives        import COPY, LINK, MOVE, TRANSFER
from .staging_directives        import SKIP_FAILED, CREATE_PARENTS

from .utils                     import version, version_short
from .utils                     import version_detail, version_branch
from .utils                     import sdist_name, sdist_path


# ------------------------------------------------------------------------------
# make submodules available -- mostly for internal use
import utils
import worker
import umgr
import pmgr
import agent


# ------------------------------------------------------------------------------

