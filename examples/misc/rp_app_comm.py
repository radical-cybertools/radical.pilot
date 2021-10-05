#!/usr/bin/env python3

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
        pdesc = rp.PilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)
        tmgr  = rp.TaskManager(session=session)
        tmgr.add_pilots(pilot)

        pwd  = os.path.dirname(os.path.abspath(__file__))
        TD  = rp.TaskDescription
        tds = list()

        for i in range(n_master):
            tds.append(TD({'executable': '%s/rp_app_master.py' % pwd,
                             'arguments' : [n_worker]}))
        for i in range(n_worker):
            tds.append(TD({'executable': '%s/rp_app_worker.py' % pwd}))

        tmgr.submit_tasks(tds)
        tmgr.wait_tasks()

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

