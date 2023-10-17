#!/usr/bin/env python3

import time
import radical.pilot as rp


# - initial ML force field exists
# - while iteration < 1000 (configurable):
#   - start model training task (ModelTrain) e.g. CVAE
#   - start force field training task (FFTrain)
#   - start MD simulation tasks, use all the available resources (MDSim)
#   - for any MD that completes
#     - start UncertaintyCheck test for it (UCCheck)
#     - if uncertainty > threshold:
#       - ADAPTIVITY GOES HERE
#       - run DFT task
#       - DFT task output -> input to FFTrain task
#         - FFTrain task has some conversion criteria.
#           If met, FFTrain task goes away
#           - kill MDSim tasks from previous iteration
#           -> CONTINUE WHILE (with new force field)
#     - else (uncertainty <= threshold):
#       - MD output -> input to ModelTrain task
#       - run new MD task / run multiple MD tasks for each structure (configurable)


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
        for i in range(30):

            td = rp.TaskDescription({'executable': '/bin/sleep',
                                     'arguments' : [str(i)],
                                     'metadata'  : {'task_type': 'type_1'}})
            tds.append(td)


        pmgr.wait_pilots(state=rp.PMGR_ACTIVE)
        tasks = tmgr.submit_tasks(tds)
        time.sleep(15)

        pilot.rpc('ddmd_deprecate', 'type_1')
        tmgr.wait_tasks()

        for task in tasks:
            print(task.uid, task.state)



        tasks = tmgr.submit_tasks(tds)
        tmgr.wait_tasks()

        for task in tasks:
            print(task.uid, task.state)


    finally:
        session.close(download=False)


# ------------------------------------------------------------------------------

