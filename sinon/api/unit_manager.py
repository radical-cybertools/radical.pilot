

from attributes import *
from constants  import *


# ------------------------------------------------------------------------------
#
class UnitManager (Attributes) :
    """ 
    UnitManager class -- manages a pool 
    """


    # --------------------------------------------------------------------------
    #
    def __init__ (self, url=None, scheduler='default', session=None) :

        Attributes.__init__ (self)


    # --------------------------------------------------------------------------
    #
    def add_pilot (self, pid, ttype=SYNC) :
        """
        add (Compute or Data)-Pilot(s) to the pool
        """

        raise Exception ("%s.add_pilot() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def list_pilots (self, ptype=ANY, ttype=SYNC) :
        """
        List IDs of data and/or compute pilots
        """

        raise Exception ("%s.list_pilots() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def remove_pilot (self, pid, drain=False, ttype=SYNC) :
        """
        Remove pilot(s) (does not cancel the pilot(s), but removes all units
        from the pilot(s).

        `drain` determines what happens to the units which are managed by the
        removed pilot(s).  If `True`, the pilot removal is delayed until all
        units reach a final state.  If `False` (the default), then `RUNNING`
        units will be canceled, and `PENDING` units will be re-assinged to the
        unit managers for re-scheduling to other pilots.
        """

        raise Exception ("%s.remove_pilot() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def submit_unit (self, description, ttype=SYNC) :
        """
        Instantiate and return (Compute or Data)-Unit object(s)
        """

        raise Exception ("%s.submit_unit() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def list_units (self, utype=ANY, ttype=SYNC) :
        """
        List IDs of data and/or compute units
        """

        raise Exception ("%s.list_units() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def get_unit (self, uids, ttype=SYNC) :
        """
        Reconnect to and return (Compute or Data)-Unit object(s)
        """

        raise Exception ("%s.get_unit() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def wait_unit (self, uids, state=[DONE, FAILED, CANCELED], timeout=-1.0, ttype=SYNC) :
        """
        Wait for given unit(s) to enter given state
        """

        raise Exception ("%s.wait_unit() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def cancel_units (self, uids, ttype=SYNC) :
        """
        Cancel given unit(s)
        """

        raise Exception ("%s.cancel_unit() is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

