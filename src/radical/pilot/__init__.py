
__copyright__ = "Copyright 2013-2024, http://radical.rutgers.edu"
__license__   = "MIT"


# ------------------------------------------------------------------------------
# we *first* import radical.utils, so that the monkeypatching of the logger has
# a chance to kick in before the logging module is pulled by any other 3rd party
# module, and also to monkeypatch `os.fork()` for the `atfork` functionality
#
import os            as _os
import radical.utils as _ru


# ------------------------------------------------------------------------------
# constants and types
from .states     import *
from .constants  import *


# ------------------------------------------------------------------------------
# import API
from .session                   import Session
from .proxy                     import Proxy
from .client                    import Client

from .task_manager              import TaskManager
from .task                      import Task
from .raptor_tasks              import RaptorMaster, RaptorWorker
from .pytask                    import PythonTask
from .task_description          import TaskDescription
from .task_description          import TASK_EXECUTABLE, TASK_SERVICE
from .task_description          import TASK_METH, TASK_METHOD
from .task_description          import TASK_FUNC, TASK_FUNCTION
from .task_description          import TASK_EXEC, TASK_EVAL
from .task_description          import TASK_PROC, TASK_SHELL
from .task_description          import RAPTOR_MASTER, RAPTOR_WORKER
from .task_description          import AGENT_SERVICE
from .resource_config           import ResourceConfig
from .resource_config           import ResourceOccupation, RO, Slot
from .resource_config           import RankRequirements
from .resource_config           import Node, NumaNode
from .resource_config           import NodeList, NumaNodeList
from .resource_config           import NumaDomainMap, NumaDomain

from .pilot_manager             import PilotManager
from .pilot                     import Pilot
from .pilot_description         import PilotDescription

pythontask = PythonTask.pythontask


# ------------------------------------------------------------------------------
# make submodules available -- mostly for internal use
from . import utils
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
_mod_root = _os.path.dirname (__file__)

version_short, version_base, version_branch, version_tag, version_detail \
             = _ru.get_version(_mod_root)
version      = version_short
__version__  = version_detail



# ------------------------------------------------------------------------------

