
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
from .proxy                     import Proxy

from .task_manager              import TaskManager
from .task                      import Task
from .raptor_tasks              import RaptorMaster, RaptorWorker
from .pytask                    import PythonTask
from .task_description          import TaskDescription
from .task_description          import TASK_EXECUTABLE
from .task_description          import TASK_METHOD, TASK_FUNC, TASK_FUNCTION
from .task_description          import TASK_EXEC, TASK_EVAL, TASK_PROC, TASK_SHELL
from .task_description          import RAPTOR_MASTER, RAPTOR_WORKER
from .task_description          import AGENT_SERVICE

from .pilot_manager             import PilotManager
from .pilot                     import Pilot
from .pilot_description         import PilotDescription

pythontask = PythonTask.pythontask


# ------------------------------------------------------------------------------
# make submodules available -- mostly for internal use
from . import utils
from . import worker
from . import tmgr
from . import pmgr
from . import agent

from .agent  import Agent_0
from .agent  import Agent_n

from .raptor import Master, Worker


# ------------------------------------------------------------------------------
#
# get version info
#
import os as _os

version_short, version_detail, version_base, version_branch, \
        sdist_name, sdist_path = _ru.get_version(_os.path.dirname(__file__))

version = version_short


# ------------------------------------------------------------------------------

