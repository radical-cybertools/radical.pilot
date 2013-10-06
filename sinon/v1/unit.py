

import sinon.api           as sa
import sinon.utils         as su
import sinon
from   attributes      import *
from   constants       import *


# ------------------------------------------------------------------------------
#
class Unit (Attributes, sa.Unit) :
    """ 
    Base class for DataUnit and ComputeUnit.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, uid, _description=None, _manager=None) : 

        # initialize session
        self._sid, self._root = sinon.initialize ()

        # FIXME: check if unit is valid
        print 'checking uid validity'

        # FIXME: reconnect to unit
        print 'reconnect to unit'

        umid = None
        if  _manager :
            umid = _manager.umid
 
        descr = None
        if  _description :
            descr = _description
 
        # initialize attributes
        Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # set basic state attributes
        self._attributes_register  (UID,          uid,   STRING, SCALAR, READONLY)
        self._attributes_register  (STATE,        None,  STRING, SCALAR, READONLY)
        self._attributes_register  (STATE_DETAIL, None,  STRING, SCALAR, READONLY)

        # set inspection attributes
        self._attributes_register  (UNIT_MANAGER, umid,  STRING, SCALAR, READONLY)
        self._attributes_register  (DESCRIPTION,  descr, STRING, SCALAR, READONLY)
        self._attributes_register  (PILOT,        None,  STRING, SCALAR, READONLY)
        self._attributes_register  (SUBMIT_TIME,  None,  TIME,   SCALAR, READONLY)
        self._attributes_register  (START_TIME,   None,  TIME,   SCALAR, READONLY)
        self._attributes_register  (END_TIME,     None,  TIME,   SCALAR, READONLY)



    # --------------------------------------------------------------------------
    #
    @classmethod
    def create (cls, description, manager) :
        """
        """

        uid = su.generate_unit_id ()

        return cls (uid, _description=description, _manager=manager)
    
    # --------------------------------------------------------------------------
    #
    def wait (self, state=[DONE, FAILED, CANCELED], timeout=None, async=False) :

        if  not isinstance (state, list) :
            state = [state]

        # FIXME: be a little more intelligent
        # FIXME: use timeouts
        # FIXME: async call...
        import time
        while self.state not in state :
            time.sleep (1)


    # --------------------------------------------------------------------------
    #
    def cancel (self, async=False) :
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

