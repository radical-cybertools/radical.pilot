#!/usr/bin/env python3

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import sys
import time
import pprint
import random

import radical.pilot as rp
import radical.utils as ru


if True:

    n_nodes =  10000
    n_tasks =  30001

    print('========================================')

    nodes = [{'index'   : i,
              'name'    : 'node_%05d' % i,
              'cores'   : [rp.ResourceOccupation(index=0, occupation=rp.DOWN),
                           rp.ResourceOccupation(index=1, occupation=rp.BUSY),
                           rp.ResourceOccupation(index=2, occupation=rp.FREE),
                           rp.ResourceOccupation(index=3, occupation=rp.FREE),
                           rp.ResourceOccupation(index=4, occupation=rp.FREE),
                           rp.ResourceOccupation(index=5, occupation=rp.FREE),
                           rp.ResourceOccupation(index=6, occupation=rp.FREE)],
              'gpus'    : [rp.ResourceOccupation(index=0, occupation=rp.FREE),
                           rp.ResourceOccupation(index=1, occupation=rp.FREE),
                           rp.ResourceOccupation(index=2, occupation=rp.DOWN)],
              'lfs'     : 1024,
              'mem'     : 1024} for i in range(n_nodes)]

    nl = rp.NodeList(nodes=[rp.NodeResources(ni) for ni in nodes])

  # pprint.pprint(nl.as_dict())

    rr     = rp.RankRequirements(n_cores=4, n_gpus=1, core_occupation=0.5)
    start  = time.time()
    allocs = list()
    for i in range(n_tasks):

        slots = nl.find_slots(rr, n_slots=2)
      # print(i, slots)
        if slots:
          # print(i)
            allocs.append(slots)

        if allocs and random.random() < 0.001:
            to_release = random.choice(allocs)
            allocs.remove(to_release)
            nl.release_slots(to_release)

    stop = time.time()
    print('find_slots: %.2f' % (stop - start))

    for slots in allocs:
        nl.release_slots(slots)

    for _ in range(5):

        slots = nl.find_slots(rp.RankRequirements(n_cores=1,
                                                  core_occupation=0.5))
        print(slots)

    print('========================================')


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    report  = ru.Reporter(name='radical.pilot')
    report.title('pilot resource example')

    session = rp.Session()

    try:
        pmgr = rp.PilotManager(session=session)
        tmgr = rp.TaskManager(session=session)

        report.header('submit pilots')

        pd_init = {'resource'      : 'local.localhost',
                   'runtime'       : 15,
                   'exit_on_error' : True,
                   'nodes'         : 2
                  }
        pdesc = rp.PilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)
        tmgr.add_pilots(pilot)

        pilot.wait([rp.PMGR_ACTIVE, rp.FAILED])
        pprint.pprint(pilot.nodelist.as_dict())

        n = 5
        report.header('submit %d tasks' % n)
        report.progress_tgt(n, label='create')

        tds = list()
        for i in range(n):
            slots = pilot.nodelist.find_slots(rp.RankRequirements(n_cores=1,
                                                                  lfs=512))
            print('=== %s' % slots)

            td = rp.TaskDescription()
            td.executable   = '/bin/date'
            td.slots        = slots

            tds.append(td)
            report.progress()

        pprint.pprint(pilot.nodelist.as_dict())

        report.progress_done()

        tasks = tmgr.submit_tasks(tds)
        tmgr.wait_tasks()

        for task in tasks:
            print('  * %s: %s [%s], %s' % (task.uid, task.state, task.exit_code,
                                           task.stdout.strip()))

    finally:
        report.header('finalize')
        session.close(download=True)

    report.header()


# ------------------------------------------------------------------------------

