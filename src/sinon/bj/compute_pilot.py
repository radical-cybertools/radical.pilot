

import pilot
import sinon.api   as sa

import bj_dummy    as bj

# ------------------------------------------------------------------------------
#
class ComputePilot (pilot.Pilot, sa.Pilot) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, pid) : 


        if  pid :
            # reconnect
            self._pilot = bj.PilotCompute (pilot_url=pid)
        else :
            self._pilot = None


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

