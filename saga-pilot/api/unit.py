

import saga.attributes as sa

from constants import *


# ------------------------------------------------------------------------------
#
class Unit (sa.Attributes) :
    """ 
    Base class for DataUnit and ComputeUnit.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, uid=None) : 

        sa.Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register  (ID,           None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register  (STATE,        None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register  (STATE_DETAIL, None, sa.STRING, sa.SCALAR, sa.READONLY)

        # deep inspection
        self._attributes_register  (UNIT_MANAGER, None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register  (PILOT,        None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register  (SUBMIT_TIME,  None, sa.TIME,   sa.SCALAR, sa.READONLY)
        self._attributes_register  (START_TIME,   None, sa.TIME,   sa.SCALAR, sa.READONLY)
        self._attributes_register  (END_TIME,     None, sa.TIME,   sa.SCALAR, sa.READONLY)
        # ...

    # --------------------------------------------------------------------------
    #
    def wait (self, state=FINAL, timeout=None, ttype=SYNC) :
        """
        block until the unit reaches the specified state, or timeout, whichever
        comes first.  Negative timeout block forever, zero/None timeout never
        block.
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel (self, ttype=SYNC) :
        """
        move the unit into Canceled state -- unless it it was in a final state,
        then state is not changed.
        """
        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

