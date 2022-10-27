

from   attributes  import *
from   constants   import *


# ------------------------------------------------------------------------------
#
class Pilot (Attributes) :
    """ 
    Base class for DataPilot and Pilot.

    Notes:
      - no direct submission, as that is equivalent to a TaskService with just
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

        `drain` determines what happens to the tasks which are managed by that
        pilot.  If `True`, the pilot's cancellation is delayed until all tasks
        reach a final state.  If `False` (the default), then `RUNNING` tasks
        will be canceled, and `PENDING` tasks will be re-assinged to their
        respective task managers for re-scheduling to other pilots.
        """

        raise Exception ("%s.cancel() is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#


