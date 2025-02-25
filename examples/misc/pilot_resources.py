#!/usr/bin/env python3

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import sys
import time
import random

import radical.pilot as rp
import radical.utils as ru


if True:

    print('benchmarking node list creation and slot allocation')

    n_nodes        =   9472
    gpus_per_node  =      8
    cores_per_node =     64
    mem_per_node   =    512
    lfs_per_node   =   1920

    n_tasks        =  10000
    ranks_per_task =      2
    cores_per_rank =      2
    gpus_per_rank  =      1
    mem_per_rank   =      0
    lfs_per_rank   =      0

  # n_nodes        =      2
  # gpus_per_node  =      2
  # cores_per_node =      8
  # mem_per_node   =    512
  # lfs_per_node   =    512
  #
  # n_tasks        =      1
  # ranks_per_task =      2
  # cores_per_rank =      2
  # gpus_per_rank  =      1
  # mem_per_rank   =      0
  # lfs_per_rank   =      0

    start = time.time()
    nodes = [rp.NodeResources({
              'index'   : i,
              'name'    : 'node_%05d' % i,
              'cores'   : [rp.RO(index=x, occupation=rp.FREE)
                                          for x in range(cores_per_node)],
              'gpus'    : [rp.RO(index=x, occupation=rp.FREE)
                                          for x in range(gpus_per_node)],
              'lfs'     : lfs_per_node,
              'mem'     : mem_per_node,
             }) for i in range(n_nodes)]
    nl   = rp.NodeList(nodes=nodes)
    stop = time.time()

    print('create nodelist: %8.2f' % (stop - start))


    rr = rp.RankRequirements(n_cores=cores_per_rank,
                             n_gpus=gpus_per_rank,
                             mem=mem_per_rank,
                             lfs=lfs_per_rank)

    allocs = list()
    start  = time.time()
    found  = 0
    missed = 0
    for i in range(n_tasks):

        slots = nl.find_slots(rr, n_slots=ranks_per_task)
        if slots:
            allocs.append(slots)

        if allocs and random.random() < 0.5:
            to_release = random.choice(allocs)
            allocs.remove(to_release)
            nl.release_slots(to_release)

        if slots:
            found += 1
          # for slot in slots:
          #     print('=== %s' % slot)
          # print()
        else:
            missed += 1
          # print('---')

    stop = time.time()
    print('find slots     : %8.2f' % (stop - start))
    print('found slots    : %6d' % found)
    print('missed slots   : %6d' % missed)

    for slots in allocs:
        nl.release_slots(slots)

  # sys.exit()


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
            slots = pilot.nodelist.find_slots(
                        rp.RankRequirements(n_cores=1, lfs=512), n_slots=2)

            print()
            if slots:
                for slot in slots:
                    print('=== %s' % slot)


            td = rp.TaskDescription()
            td.executable     = '/bin/date'
            td.ranks          = 2
            td.cores_per_rank = 2
            td.slots          = slots

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

