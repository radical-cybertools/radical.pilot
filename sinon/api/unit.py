

from   attributes  import *
from   constants   import *


# ------------------------------------------------------------------------------
#
class Unit (Attributes) :
    """ 
    Base class for DataUnit and ComputeUnit.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, uid=None) : 

        Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register  (UID,          None, STRING, SCALAR, READONLY)
        self._attributes_register  (STATE,        None, STRING, SCALAR, READONLY)
        self._attributes_register  (STATE_DETAIL, None, STRING, SCALAR, READONLY)

        # deep inspection
        self._attributes_register  (UNIT_MANAGER, None, STRING, SCALAR, READONLY)
        self._attributes_register  (PILOT,        None, STRING, SCALAR, READONLY)
        self._attributes_register  (SUBMIT_TIME,  None, TIME,   SCALAR, READONLY)
        self._attributes_register  (START_TIME,   None, TIME,   SCALAR, READONLY)
        self._attributes_register  (END_TIME,     None, TIME,   SCALAR, READONLY)
        # ...

    # --------------------------------------------------------------------------
    #
    def wait (self, state=FINAL, timeout=None, ttype=SYNC) :
        """
        :param state:  the state to wait for
        :type  state:  enum `state` (PENDING, ACTIVE, DONE, FAILED, CANCELED, UNKNOWN)
        :returns   :  Nothing, or a Task on ASYNC calls
        :rtype     :  None or Task
        :raises    :  BadParameter (on invalid initialization)

        Block until the unit reaches the specified state, or timeout, whichever
        comes first.  Negative timeout block forever, zero/None timeout never
        block.
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel (self, ttype=SYNC) :
        """
        :param state:  the state to wait for
        :type  state:  enum `state` (PENDING, ACTIVE, DONE, FAILED, CANCELED, UNKNOWN)
        :param ttype:  method type
        :type  ttype:  enum `ttype` (SYNC, ASYNC)
        :returns   :  Nothing, or a Task on ASYNC calls
        :rtype     :  None or Task
        :raises    :  BadParameter (on invalid initialization)

        Move the unit into Canceled state -- unless it it was in a final state,
        then state is not changed.
        """
        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

