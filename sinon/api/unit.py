

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
    def __init__ (self, uid) : 

        Attributes.__init__ (self)


    # --------------------------------------------------------------------------
    #
    def wait (self, state=[DONE, FAILED, CANCELED], timeout=None, async=False) :
        """
        :param state:  the state to wait for
        :type  state:  enum `state` (PENDING, ACTIVE, DONE, FAILED, CANCELED, UNKNOWN)
        :returns:      Nothing, or a Task on ASYNC calls
        :rtype:        None or Task
        :raises:       BadParameter (on invalid initialization)

        Block until the unit reaches the specified state, or timeout, whichever
        comes first.  Negative timeout block forever, zero/None timeout never
        block.
        """

        raise Exception ("%s.wait() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def cancel (self, async=False) :
        """
        :param state:  the state to wait for
        :type  state:  enum `state` (PENDING, ACTIVE, DONE, FAILED, CANCELED, UNKNOWN)
        :param async:  flag for asynchronous method invocation
        :type  async:  bool
        :returns   :  Nothing, or a Task on ASYNC calls
        :rtype     :  None or Task
        :raises    :  BadParameter (on invalid initialization)

        Move the unit into Canceled state -- unless it it was in a final state,
        then state is not changed.
        """

        raise Exception ("%s.cancel() is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

