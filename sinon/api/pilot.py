

from   attributes  import *
from   constants   import *


# ------------------------------------------------------------------------------
#
class Pilot (Attributes) :
    """ 
    Base class for DataPilot and ComputePilot.

    Notes:
      - no direct submission, as that is equivalent to a UnitService with just
        one pilot.

    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pid) : 

        Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register  (PID,           None, STRING, SCALAR, READONLY)
        self._attributes_register  (DESCRIPTION,   None, ANY,    SCALAR, READONLY)
        self._attributes_register  (STATE,         None, STRING, SCALAR, READONLY)
        self._attributes_register  (STATE_DETAIL,  None, STRING, SCALAR, READONLY)

        # deep inspection
        self._attributes_register  (UNITS,         None, STRING, VECTOR, READONLY)
        self._attributes_register  (UNIT_MANAGERS, None, STRING, VECTOR, READONLY)
        self._attributes_register  (PILOT_MANAGER, None, STRING, SCALAR, READONLY)
        # ...

    # --------------------------------------------------------------------------
    #
    def wait (self, state=FINAL, timeout=None, ttype=SYNC) :
        """
        block until the pilot reaches the specified state, or timeout, whichever
        comes first.  Negative timeout block forever, zero/None timeout never
        block.
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel (self, ttype=SYNC) :
        """
        move the pilot into Canceled state -- unless it it was in a final state,
        then state is not changed.
        """
        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

