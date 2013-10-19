

import sinon.api       as sa
from   pilot       import Pilot


# ------------------------------------------------------------------------------
#
class DataPilot (Pilot, sa.Pilot) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pid) : 

        Pilot.__init__ (self, pid)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

