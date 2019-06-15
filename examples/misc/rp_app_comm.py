#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'


import os
import radical.pilot as rp

n_master =  1
n_worker =  16


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    session = rp.Session()

    try:
        pmgr    = rp.PilotManager(session=session)
        pd_init = {'resource'      : 'local.localhost',
                   'runtime'       : 60,
                   'exit_on_error' : True,
                   'cores'         : n_master * 1 + n_worker * 1,
                   'app_comm'      : ['app_pubsub', 
                                      'work_queue', 
                                      'result_queue']
                  }
        pdesc = rp.ComputePilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)
        umgr  = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        pwd  = os.path.dirname(os.path.abspath(__file__))
        CUD  = rp.ComputeUnitDescription
        cuds = list()

        for i in range(n_master):
            cuds.append(CUD({'executable': '%s/rp_app_master.py' % pwd,
                             'arguments' : [n_worker]}))
        for i in range(n_worker):
            cuds.append(CUD({'executable': '%s/rp_app_worker.py' % pwd}))

        umgr.submit_units(cuds)
        umgr.wait_units()

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

