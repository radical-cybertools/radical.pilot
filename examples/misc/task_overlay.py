#!/usr/bin/env python3

import os
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    here    = os.path.abspath(os.path.dirname(__file__))
    master  = '%s/task_overlay_master.py' % here
    worker  = '%s/task_overlay_worker.py' % here

    session = rp.Session()
    try:
        pd = {'resource'   : 'local.debug',
              'cores'      : 128,
              'runtime'    : 60}

        td = {'executable' : master,
              'arguments'  : [worker]}

        pmgr  = rp.PilotManager(session=session)
        umgr  = rp.UnitManager(session=session)
        pilot = pmgr.submit_pilots(rp.ComputePilotDescription(pd))
        task  = umgr.submit_units(rp.ComputeUnitDescription(td))

        umgr.add_pilots(pilot)
        umgr.wait_units()

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

