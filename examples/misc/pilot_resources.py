#!/usr/bin/env python3

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import sys
import time
import random

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
def benchmark(reporter):

    reporter.title('pilot resource management - benchmark')

    report.header('benchmark node list creation')

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
    nodes = [rp.Node({
              'index'   : i,
              'name'    : 'node_%05d' % i,
              'cores'   : [rp.RO(index=x, occupation=rp.FREE)
                                          for x in range(cores_per_node)],
              'gpus'    : [rp.RO(index=x, occupation=rp.FREE)
                                          for x in range(gpus_per_node)],
              'lfs'     : lfs_per_node,
              'mem'     : mem_per_node,
             }) for i in range(n_nodes)]
    nl = rp.NodeList(nodes=nodes)
    stop = time.time()

  # numa_domain_map = {0: rp.NumaDomain(cores=range(0, 4), gpus=[0]),
  #                    1: rp.NumaDomain(cores=range(4, 8), gpus=[1])}
  # nnl = rp.NumaNodeList(nodes=[rp.NumaNode(node, numa_domain_map)
  #                              for node in nodes])
  #
  # import pprint
  # pprint.pprint(nnl.as_dict())
  # pprint.pprint(nnl.nodes[0].as_dict())

    report.ok('nodelist      : %8.2f sec / %d nodes\n' % (stop - start, n_nodes))
    report.ok('                %8.2f nodes / sec\n' % (n_nodes / (stop - start)))

    report.header('benchmark slot allocation')
    rr = rp.RankRequirements(n_cores=cores_per_rank,
                             n_gpus=gpus_per_rank,
                             mem=mem_per_rank,
                             lfs=lfs_per_rank)

    allocs = list()
    start  = time.time()
    hits   = 0
    free   = 0
    miss   = 0
    for i in range(n_tasks):

        slots = nl.find_slots(rr, n_slots=ranks_per_task)
        if slots:
            hits += 1
            allocs.append(slots)
        else:
            miss += 1

        if allocs and random.random() < 0.5:
            free += 1
            to_release = random.choice(allocs)
            allocs.remove(to_release)
            nl.release_slots(to_release)

    stop = time.time()
    report.ok('find slots    : %8.2f sec / %d slots\n'
              % (stop - start, n_tasks * ranks_per_task))
    report.ok('                %8.2f slots / sec\n'
              % (n_tasks * ranks_per_task / (stop - start)))
    report.ok('hits/miss/free: %8d / %d / %d\n' % (hits, miss, free))

    for slots in allocs:
        nl.release_slots(slots)

    report.header('')

  # sys.exit()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    report = ru.Reporter(name='radical.pilot')

    if True:
        benchmark(report)
        sys.exit()

    report.title('pilot resource management example')

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

