#!/usr/bin/env python3

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
def task_state_cb(task, state):

    print('  task %-30s: %s' % (task.uid, task.state))


# ------------------------------------------------------------------------------
#
@rp.pythontask
def hello_world(msg, sleep):
    import time
    print('hello %s: %.3f' % (msg, time.time()))
    time.sleep(sleep)
    print('hello %s: %.3f' % (msg, time.time()))
    return 'hello %s' % msg


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

        td_func_1 = rp.TaskDescription({'mode': rp.TASK_FUNCTION,
                                        'function': hello_world('client', 3)})

        td_func_2 = rp.TaskDescription({'mode': rp.TASK_FUNCTION,
                                        'function': 'hello',
                                        'args'    : ['raptor', 3]})
        tasks = raptor.submit_tasks([td_func_1, td_func_2])

        tmgr.wait_tasks([task.uid for task in tasks])

        for task in tasks:
            print('%s [%s]:\n%s' % (task.uid, task.state, task.stdout))

        raptor.rpc('stop')
        tmgr.wait_tasks(raptor.uid)
        print('%s [%s]: %s' % (raptor.uid, raptor.state, raptor.stdout))

    finally:
        session.close(download=False)


# ------------------------------------------------------------------------------

