# pylint: disable=import-error

from constants                     import *
from exceptions                    import *
from session                       import Session
from context                       import Context
from url                           import Url
from callback                      import Callback
from attributes                    import Attributes
from description                   import Description

from task_description              import TaskDescription
from data_task_description         import DataTaskDescription

from task                          import Task
from data_task                     import DataTask

from pilot_description             import PilotDescription
from data_pilot_description        import DataPilotDescription

from pilot                         import Pilot
from data_pilot                    import DataPilot

from pilot_manager                 import PilotManager
from task_manager                  import TaskManager


# ------------------------------------------------------------------------------
#



import os
import subprocess    as sp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
version = "unknown"

try :
    cwd     = os.path.dirname (os.path.abspath (__file__))
    fn      = os.path.join    (cwd, '../VERSION')
    version = ru.ru_open(fn).read ().strip ()

    p   = sp.Popen (['git', 'describe', '--tags', '--always'],
                    stdout=sp.PIPE)
    out = p.communicate()[0]

    if  out and not p.returncode :
        version += '-' + out.strip()

except Exception :
    pass


# ------------------------------------------------------------------------------
#


