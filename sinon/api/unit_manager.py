

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

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # deep inspection
        self._attributes_register  (SCHEDULER, None, STRING, SCALAR, READONLY)
        self._attributes_register  (PILOTS,    None, STRING, VECTOR, READONLY)
        self._attributes_register  (UNITS,     None, STRING, VECTOR, READONLY)
        # ...


    # --------------------------------------------------------------------------
    #
    def add_pilot (self, pid, ttype=SYNC) :
        """
        add (Compute or Data)-Pilot(s) to the pool
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def list_pilots (self, ptype=ANY, ttype=SYNC) :
        """
        List IDs of data and/or compute pilots
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def remove_pilot (self, pid, drain=True, ttype=SYNC) :
        """
        Remove pilot(s) (does not cancel the pilot(s), but removes all units
        from the pilot(s).
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def submit_unit (self, description, ttype=SYNC) :
        """
        Instantiate and return (Compute or Data)-Unit object(s)
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def list_units (self, utype=ANY, ttype=SYNC) :
        """
        List IDs of data and/or compute units
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def get_unit (self, uids, ttype=SYNC) :
        """
        Reconnect to and return (Compute or Data)-Unit object(s)
        """
        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def wait_units (self, uids, state=FINAL, timeout=-1.0, ttype=SYNC) :
        """
        Cancel given unit(s)
        """

    # --------------------------------------------------------------------------
    #
    def cancel_units (self, uids, ttype=SYNC) :
        """
        Cancel (set of) given unit(s)
        """

# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

