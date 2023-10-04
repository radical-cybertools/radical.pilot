#!/usr/bin/env python3

import time
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # use the resource specified as argument, fall back to localhost
    session = rp.Session()

    try:
        pmgr   = rp.PilotManager(session=session)
        tmgr   = rp.TaskManager(session=session)

        pd_init = {'resource': 'local.debug',
                   'runtime' : 15,
                   'cores'   : 64}
        pdesc = rp.PilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)
        tmgr.add_pilots(pilot)

        tds = list()
        for i in range(10):

            td = rp.TaskDescription({'executable': '/bin/sleep',
                                     'arguments' : ['10'],
                                     'metadata'  : {'task_type': 'type_1'}})
            tds.append(td)


        pmgr.wait_pilots(state=rp.PMGR_ACTIVE)
        tmgr.submit_tasks(tds)
        time.sleep(3)

        pilot.rpc('ddmd_deprecate', 'type_1')
        tmgr.wait_tasks()

    finally:
        session.close(download=False)


# ------------------------------------------------------------------------------

