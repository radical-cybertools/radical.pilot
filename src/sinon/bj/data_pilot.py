

import pilot
import sinon.api  as sa


# ------------------------------------------------------------------------------
#
class DataPilot (pilot.Pilot, sa.Pilot) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pid) : 

        pilot.Pilot.__init__ (self, pid)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

