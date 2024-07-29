#!/usr/bin/env python3

import radical.pilot as rp


def state_cb(task, state):
    if state in rp.FINAL:
        print('%s [%d]: %s' % (task.uid, task.description.priority, task.state))


if __name__ == '__main__':

    session = rp.Session()
    try:

        pmgr  = rp.PilotManager(session=session)
        tmgr  = rp.TaskManager(session=session)
        pdesc = rp.PilotDescription({'resource': 'local.localhost',
                                     'runtime' : 1024 * 1024,
                                     'nodes'   : 1})
        pilot = pmgr.submit_pilots(pdesc)

        tmgr.add_pilots(pilot)
        tmgr.register_callback(state_cb)

        n   = 200
        tds = list()
        for i in range(n):
            td = rp.TaskDescription({'executable': 'true',
                                     'priority'  : i % 3})
            tds.append(td)

        tasks = tmgr.submit_tasks(tds)

        tmgr.wait_tasks()

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

