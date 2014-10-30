

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
    def wait (self, state=[DONE, FAILED, CANCELED], timeout=None) :
        """
        block until the pilot reaches the specified state, or timeout, whichever
        comes first.  Negative timeout block forever, zero/None timeout never
        block.
        """

        raise Exception ("%s.wait() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def cancel (self, drain=False) :
        """
        Move the pilot into Canceled state -- unless it it was in a final state,
        then state is not changed.

        `drain` determines what happens to the units which are managed by that
        pilot.  If `True`, the pilot's cancellation is delayed until all units
        reach a final state.  If `False` (the default), then `RUNNING` units
        will be canceled, and `PENDING` units will be re-assinged to their
        respective unit managers for re-scheduling to other pilots.
        """

        raise Exception ("%s.cancel() is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#


