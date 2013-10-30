

# we first completely load the API definition classes, and then overload the
# here implemented classes.  That way we import the complete (but not
# implemented) API, and only overload what actually exists here in v1:

from sinon._api import *


from sinon.v2.exceptions                  import *
from sinon.v2.session                     import Session
from sinon.v2.pilot                       import Pilot
from sinon.v2.pilot_manager               import PilotManager
from sinon.v2.unit_manager                import UnitManager
from sinon.v2.compute_unit_description    import ComputeUnitDescription
from sinon.v2.compute_pilot_description   import ComputePilotDescription



