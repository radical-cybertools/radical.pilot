

import unit        as u
import sinon._api    as sa


# ------------------------------------------------------------------------------
#
class DataUnit (u.Unit, sa.DataUnit) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, uid, _description=None, _manager=None) : 

        u.Unit.__init__ (self, uid, _description, _manager)


    # --------------------------------------------------------------------------
    #
    def import_data (self, src) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def export_data (self, tgt) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def remove_data (self) :

        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

