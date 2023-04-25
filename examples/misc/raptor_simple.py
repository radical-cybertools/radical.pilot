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

        raptor = pilot.submit_raptors(rp.TaskDescription(
            {'mode': rp.RAPTOR_MASTER}))[0]

        worker = raptor.submit_workers(rp.TaskDescription(
            {'mode': rp.RAPTOR_WORKER}))[0]

        task = raptor.submit_tasks(rp.TaskDescription(
            {'mode': rp.TASK_PROC,
             'executable': 'date'}))[0]

        tmgr.wait_tasks(task.uid)
        print('%s [%s]: %s' % (task.uid, task.state, task.stdout))

        raptor.rpc('stop')
        tmgr.wait_tasks(raptor.uid)
        print('%s [%s]: %s' % (raptor.uid, raptor.state, raptor.stdout))

    finally:
        session.close(download=False)


# ------------------------------------------------------------------------------

