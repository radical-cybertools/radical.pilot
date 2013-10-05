

import sinon.api     as sa
import unit          as u
from   constants import *


# ------------------------------------------------------------------------------
#
class DataUnit (u.Unit, sa.DataUnit) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, uid) : 

        u.Unit.__init__ (self, uid)


    # --------------------------------------------------------------------------
    #
    def import_data (self, src, ttype=SYNC) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def export_data (self, tgt, ttype=SYNC) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def remove_data (self, ttype=SYNC) :

        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

