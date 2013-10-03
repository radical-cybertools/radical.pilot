

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


    # --------------------------------------------------------------------------
    #
    def wait (self, state=[DONE, FAILED, CANCELED], timeout=None, ttype=SYNC) :
        """
        block until the pilot reaches the specified state, or timeout, whichever
        comes first.  Negative timeout block forever, zero/None timeout never
        block.
        """

        raise Exception ("%s.wait() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def cancel (self, ttype=SYNC) :
        """
        move the pilot into Canceled state -- unless it it was in a final state,
        then state is not changed.
        """

        raise Exception ("%s.cancel() is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

