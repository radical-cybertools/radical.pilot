

import sinon.api       as sa
from   attributes  import *
from   constants   import *


# ------------------------------------------------------------------------------
#
class Pilot (Attributes, sa.Pilot) :

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
    def wait (self, state=[DONE, FAILED, CANCELED], timeout=None, ttype=SYNC) :

        if  not isinstance (state, list) :
            state = [state]

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel (self, ttype=SYNC) :

        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

