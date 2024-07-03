#!/usr/bin/env python3

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import sys
import time
import random

import radical.pilot as rp
import radical.utils as ru


if True:

    n_nodes        =   9472
    gpus_per_node  =      8
    cores_per_node =     64
    mem_per_node   =    512
    lfs_per_node   =   1920

    n_tasks        =  10000
    ranks_per_task =      2
    cores_per_task =     16
    gpus_per_task  =      2
    mem_per_task   =      0
    lfs_per_task   =      0

    nodes = [{'index'   : i,
              'name'    : 'node_%05d' % i,
              'cores'   : [rp.RO(index=x, occupation=rp.FREE)
                                          for x in range(cores_per_node)],
              'gpus'    : [rp.RO(index=x, occupation=rp.FREE)
                                          for x in range(gpus_per_node)],
              'lfs'     : lfs_per_node,
              'mem'     : mem_per_node
             } for i in range(n_nodes)]

    nl = rp.NodeList(nodes=[rp.NodeResources(ni) for ni in nodes])
    rr = rp.RankRequirements(n_cores=cores_per_task,
                             n_gpus=gpus_per_task,
                             mem=mem_per_task,
                             lfs=lfs_per_task)

    allocs = list()
    start  = time.time()
    for i in range(n_tasks):

        slots = nl.find_slots(rr, n_slots=ranks_per_task)
        if slots:
            allocs.append(slots)

        if allocs and random.random() < 0.5:
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

    sys.exit()


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

        pd_init = {'resource': 'local.localhost',
                   'runtime' : 15,
                   'nodes'   : 1}
        pdesc = rp.PilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)

        tmgr.add_pilots(pilot)
        pilot.wait([rp.PMGR_ACTIVE, rp.FAILED])

        n = 1
        report.header('submit %d tasks' % n)

        tds = list()
        for i in range(n):
          # slots = pilot.nodelist.find_slots(rp.RankRequirements(n_cores=1,
          #                                                       lfs=512),
          #                                   n_slots=2)
          #
          # print()
          # if slots:
          #     for slot in slots:
          #         print('=== %s' % slots)


            td = rp.TaskDescription()
            td.executable     = '/bin/date'
            td.ranks          = 2
            td.cores_per_rank = 2
          # td.slots          = slots

            tds.append(td)

        tasks = tmgr.submit_tasks(tds)
        tmgr.wait_tasks()

        import pprint
        for task in tasks:
            print('  * %s: %s [%s], %s' % (task.uid, task.state, task.exit_code,
                                           task.stdout.strip()))

            print()
            for slot in task.slots:
                print(slot)

    finally:
        report.header('finalize')
        session.close(download=True)

    report.header()


# ------------------------------------------------------------------------------

