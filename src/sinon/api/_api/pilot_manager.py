

from attributes import *
from constants  import *


# ------------------------------------------------------------------------------
#
class PilotManager (Attributes) :
    """ 
    PilotManager class.

    Notes:
      - cancel() not needed if PM is not a service, i.e. does not have state.
    """


    # --------------------------------------------------------------------------
    #
    def __init__ (self, url=None, session=None) : 

        Attributes.__init__ (self)


    # --------------------------------------------------------------------------
    #
    def submit_pilot (self, description) :
        """
        Instantiate and return (Compute or Data)-Pilot object
        """

        raise Exception ("%s.submit_pilot() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def list_pilots (self) :
        """
        Returns pids of known pilots.
        """

        raise Exception ("%s.list_pilots() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def get_pilot (self, pids) :
        """
        Reconnect to and return (Compute or Data)-Pilot object(s)
        """

        raise Exception ("%s.get_pilot() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def wait_pilot (self, pids, state=[DONE, FAILED, CANCELED], timeout=-1.0) :
        """
        Wait for given pilot(s).
        """

        raise Exception ("%s.wait_pilot() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def cancel_pilot (self, pids) :
        """
        Cancel given pilot(s).
        """

        raise Exception ("%s.cancel_pilot() is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#


