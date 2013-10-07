

import sinon.api       as sa
import sinon
from   attributes import *
from   constants  import *


# ------------------------------------------------------------------------------
#
class UnitManager (Attributes, sa.UnitManager) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, url=None, scheduler='default', session=None) :

        # initialize session
        self._sid, self._root = sinon.initialize ()

        # initialize attributes
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
    def add_pilot (self, pid, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def list_pilots (self, ptype=ANY, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def remove_pilot (self, pid, drain=True, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def submit_unit (self, descr, async=False) :

        # FIXME: async, bulk

        if  not descr.attribute_exists ('dtype') :
            raise sinon.BadParameter ("Invalid description (no type)")

        if  descr.dtype == sinon.COMPUTE :
            return sinon.ComputeUnit.create (descr, self)

        if  descr.dtype == sinon.DATA :
            return sinon.DataUnit.create (descr, self)

        raise sinon.BadParameter ("Unknown description type %s" % descr.dtype)


    # --------------------------------------------------------------------------
    #
    def list_units (self, utype=ANY, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def get_unit (self, uids, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def wait_units (self, uids, state=[DONE, FAILED, CANCELED], timeout=-1.0, async=False) :

        if  not isinstance (state, list) :
            state = [state]

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel_units (self, uids, async=False) :

        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

