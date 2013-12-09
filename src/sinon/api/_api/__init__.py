

from constants                     import *
from exceptions                    import *
from session                       import Session
from context                       import Context
from url                           import Url
from callback                      import Callback
from attributes                    import Attributes
from description                   import Description

from compute_unit_description      import ComputeUnitDescription
from data_unit_description         import DataUnitDescription

from unit                          import Unit
from compute_unit                  import ComputeUnit
from data_unit                     import DataUnit

from compute_pilot_description     import ComputePilotDescription
from data_pilot_description        import DataPilotDescription

from pilot                         import Pilot
from compute_pilot                 import ComputePilot
from data_pilot                    import DataPilot

from pilot_manager                 import PilotManager
from unit_manager                  import UnitManager


# ------------------------------------------------------------------------------
#



import os
import subprocess as sp


# ------------------------------------------------------------------------------
#
version = "unknown"

try :
    cwd     = os.path.dirname (os.path.abspath (__file__))
    fn      = os.path.join    (cwd, '../VERSION')
    version = open (fn).read ().strip ()

    p   = sp.Popen (['git', 'describe', '--tags', '--always'],
                    stdout=sp.PIPE)
    out = p.communicate()[0]

    # ignore pylint error on p.returncode -- false positive
    if  out and not p.returncode :
        version += '-' + out.strip()

except Exception :
    pass


# ------------------------------------------------------------------------------
#


