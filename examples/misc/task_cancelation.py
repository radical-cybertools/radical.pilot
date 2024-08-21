#!/usr/bin/env python3

import os
import time

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # demonstrate task cancelation during execution: we start some tasks, then
    # wait for some to finish, and cancel some more, both tasks currently
    # executing and tasks still pending for execution

    session = rp.Session()

    try:
        pmgr    = rp.PilotManager(session=session)
        pd_init = {'resource': 'local.localhost',
                   'runtime' : 60,
                   'cores'   :  8
                  }
        pdesc = rp.PilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)
        tmgr  = rp.TaskManager(session=session)
        tmgr.add_pilots(pilot)

        # wait until pilot is active to have somewhat sensible timings below
        pilot.wait(rp.PMGR_ACTIVE)
        print('pilot state: %s' % pilot.state)

        # submit 32 tasks, each running for 10 seconds.  We have 8 cores, so the
        # tasks will run in 4 batches of 8 at a time.
        t_start = time.time()
        tds     = list()
        for _ in range(32):

            td = rp.TaskDescription()
            td.executable = 'radical-pilot-hello.sh'
            td.arguments  = [10]
            tds.append(td)

        tasks = tmgr.submit_tasks(tds)

        # wait until 8 tasks are done (16 cores: should take about 10 seconds
        print('%d total  - wait for 8 tasks' % len(tasks))
        while True:
            time.sleep(1)
            states = [t.state for t in tasks]
            count  = len([s for s in states if s in rp.FINAL])
            if count >= 8:
                break
        print('%d DONE out of %d total' % (len(states), count))

        t_gen_1 = time.time()

        # cancel 12 tasks which are not in the set of DONE tasks above.  That
        # should request cancellation for a mix of currently running and still
        # pending tasks (8 are DONE, 8 are running right now, 16 are pending.
        done_uids   = [t.uid for t in tasks if t.state == rp.DONE]
        cancel_uids = [t.uid for t in tasks if t.uid not in done_uids]
        cancel_uids = cancel_uids[:12]
        n_canceled  = len(cancel_uids)

        tmgr.cancel_tasks(uids=cancel_uids)

        t_cancel = time.time()

        # 12 tasks should be left which fit on the available cores in two
        # batches (8 + 4), and we should wait for about 20 seconds give or take
        tmgr.wait_tasks()
        t_done = time.time()

        # print some statistocs
        states = [t.state for t in tasks]
        print('canceled %d tasks' % n_canceled)
        print('states:')
        for s in [rp.DONE, rp.FAILED, rp.CANCELED]:
            print ('  %-10s: %3d' % (s, states.count(s)))

        print('\ntimes:')
        print('  gen 1 : %10.1f sec (exp: 10 sec)' % (t_gen_1  - t_start))
        print('  cancel: %10.1f sec (exp:  0 sec)' % (t_cancel - t_gen_1))
        print('  gen x : %10.1f sec (exp: 20 sec)' % (t_done   - t_cancel))

        print()
        for task in tasks:
            print('task %s: %s' % (task.uid, task.state))

    finally:
        session.close()


# ------------------------------------------------------------------------------

