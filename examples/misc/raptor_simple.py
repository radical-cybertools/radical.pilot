#!/usr/bin/env python3

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
def task_state_cb(task, state):

    print('  task %-30s: %s' % (task.uid, task.state))


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    session = rp.Session()
    try:
        pd = rp.PilotDescription()
        pd.resource = 'local.localhost'
        pd.runtime  =  10
        pd.cores    = 128

        pmgr = rp.PilotManager(session=session)
        tmgr = rp.TaskManager(session=session)
        tmgr.register_callback(task_state_cb)

        pilot = pmgr.submit_pilots([pd])[0]
        tmgr.add_pilots(pilot)

        raptor = pilot.submit_raptors(rp.TaskDescription())[0]
        worker = raptor.submit_workers(rp.TaskDescription())[0]
        task   = raptor.submit_tasks(rp.TaskDescription(
            {'command': 'date',
             'mode'   : rp.TASK_SHELL,
             'uid'    : 'raptor_task.0000'}))[0]

        tmgr.wait_tasks(task.uid)
        print('%s [%s]: %s' % (task.uid, task.state, task.stdout))

        worker.cancel()
        tmgr.wait_tasks(worker.uid)
        print('%s [%s]: %s' % (worker.uid, worker.state, worker.stdout))

        raptor.cancel()
        tmgr.wait_tasks(raptor.uid)
        print('%s [%s]: %s' % (raptor.uid, raptor.state, raptor.stdout))

    finally:
        session.close(download=False)


# ------------------------------------------------------------------------------

