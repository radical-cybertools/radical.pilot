

import sinon.api       as sa
from   attributes import *
from   constants  import *


# ------------------------------------------------------------------------------
#
class UnitManager (Attributes, sa.UnitManager) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, url=None, scheduler='default', session=None) :

        Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # deep inspection
        self._attributes_register  (SCHEDULER, scheduler, STRING, SCALAR, READONLY)
        self._attributes_register  (PILOTS,    [],        STRING, VECTOR, READONLY)
        self._attributes_register  (UNITS,     [],        STRING, VECTOR, READONLY)
        # ...


    # --------------------------------------------------------------------------
    #
    def add_pilot (self, pid, ttype=SYNC) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def list_pilots (self, ptype=ANY, ttype=SYNC) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def remove_pilot (self, pid, drain=True, ttype=SYNC) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def submit_unit (self, description, ttype=SYNC) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def list_units (self, utype=ANY, ttype=SYNC) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def get_unit (self, uids, ttype=SYNC) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def wait_units (self, uids, state=[DONE, FAILED, CANCELED], timeout=-1.0, ttype=SYNC) :

        if  not isinstance (state, list) :
            state = [state]

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel_units (self, uids, ttype=SYNC) :

        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

