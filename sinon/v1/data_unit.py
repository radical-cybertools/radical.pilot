

import sinon.api     as sa
import unit          as u
from   constants import *


# ------------------------------------------------------------------------------
#
class DataUnit (u.Unit, sa.DataUnit) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, uid, _description=None, _manager=None) : 

        u.Unit.__init__ (self, uid, _description, _manager)


    # --------------------------------------------------------------------------
    #
    def import_data (self, src, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def export_data (self, tgt, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def remove_data (self, async=False) :

        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

