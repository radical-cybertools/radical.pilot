

import sinon._api  as sa
import pilot       as p


# ------------------------------------------------------------------------------
#
class DataPilot (p.Pilot, sa.Pilot) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pid) : 

        p.Pilot.__init__ (self, pid)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

