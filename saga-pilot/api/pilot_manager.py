

from constants import *


# ------------------------------------------------------------------------------
#
class PilotManager (object) :
    """ 
    PilotManager class.

    Notes:
      - cancel() not needed if PM is not a service, i.e. does not have state.
    """


    # --------------------------------------------------------------------------
    #
    def __init__ (self, url=None, session=None) : 
        # URL      not needed for modular arch
        # cancel() not needed for module based arch
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def submit_pilot (self, description, ttype=SYNC) :
        """
        Instantiate and return (Compute or Data)-Pilot object
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def list_pilots (self, ttype=SYNC) :
        """
        Returns pids of known pilots.
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def get_pilot (self, pids, ttype=SYNC) :
        """
        Reconnect to and return (Compute or Data)-Pilot object(s)
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def wait_pilot (self, pids, state=FINAL, timeout=-1.0, ttype=SYNC) :
        """
        Wait for given pilot(s).
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel_pilot (self, pids, ttype=SYNC) :
        """
        Cancel given pilot(s).
        """
        # FIXME
        pass



# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

