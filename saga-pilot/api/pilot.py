

import saga.attributes as sa

from constants import *


# ------------------------------------------------------------------------------
#
class Pilot (sa.Attributes) :
    """ 
    Base class for DataPilot and ComputePilot.

    Notes:
      - no direct submission, as that is equivalent to a UnitService with just
        one pilot.

    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pid) : 

        sa.Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register  (PID,           None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register  (DESCRIPTION,   None, sa.ANY,    sa.SCALAR, sa.READONLY)
        self._attributes_register  (MANAGER,       None, sa.ANY,    sa.SCALAR, sa.READONLY)
        self._attributes_register  (STATE,         None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register  (STATE_DETAIL,  None, sa.STRING, sa.SCALAR, sa.READONLY)

        # deep inspection
        self._attributes_register  (UNITS,         None, sa.STRING, sa.VECTOR, sa.READONLY)
        self._attributes_register  (UNIT_MANAGERS, None, sa.STRING, sa.VECTOR, sa.READONLY)
        self._attributes_register  (PILOT_MANAGER, None, sa.STRING, sa.SCALAR, sa.READONLY)
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

