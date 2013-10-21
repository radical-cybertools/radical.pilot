

import pilot       as p
import sinon._api  as sa


# ------------------------------------------------------------------------------
#
class ComputePilot (p.Pilot, sa.Pilot) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pid) : 

        p.Pilot.__init__ (self, pid)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

