#!/usr/bin/env python3

import os
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    n_nodes = 4
    cpn     = 8  # cores per node
    gpn     = 1  # gpus  per node

    here    = os.path.abspath(os.path.dirname(__file__))
    master  = '%s/task_overlay_master.py' % here
    worker  = '%s/task_overlay_worker.py' % here

    session = rp.Session()
    try:
        pd = {'resource'   : 'local.debug',
              'cores'      : n_nodes * cpn,
              'gpus'       : n_nodes * gpn,
              'runtime'    : 60}

        td = {'executable' : master,
              'arguments'  : [worker, n_nodes, cpn, gpn]}

        pmgr  = rp.PilotManager(session=session)
        umgr  = rp.UnitManager(session=session)
        pilot = pmgr.submit_pilots(rp.ComputePilotDescription(pd))
        task  = umgr.submit_units(rp.ComputeUnitDescription(td))

        umgr.add_pilots(pilot)
        umgr.wait_units()

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

