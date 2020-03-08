#!/usr/bin/env python3

import os
import sys
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    session = rp.Session()
    master  = os.path.basename(sys.argv[1])
    worker  = os.path.basename(sys.argv[2])
    mpath   = os.path.abspath(os.path.dirname(sys.argv[1]))
    wpath   = os.path.abspath(os.path.dirname(sys.argv[2]))

    try:
        pd = {'resource'   : 'local.debug',
              'cores'      : 128,
              'runtime'    : 60}

        td = {'executable' : '%s/%s' % (mpath, master),
              'arguments'  : '%s/%s' % (wpath, worker)}

        pmgr  = rp.PilotManager(session=session)
        umgr  = rp.UnitManager(session=session)
        pilot = pmgr.submit_pilots(rp.ComputePilotDescription(pd))
        task  = umgr.submit_units(rp.ComputeUnitDescription(td))

        umgr.add_pilots(pilot)
        umgr.wait_units()

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

