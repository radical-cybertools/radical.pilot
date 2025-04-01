#!/usr/bin/env python3

import sys
import math
import random

# import radical.pilot as rp

def get_tasks(cpn, gpn, n_nodes, n_tasks):

    cores = cpn
    gpus  = gpn
    ranks = n_nodes
    tasks = list()

    while cores > 0 and gpus > 0:
        # deplete GPUs within n_tasks
        gpr = math.floor(gpn / n_tasks)

        # more cores than 50% of remaining cores to spread ranks across nodes
        cpr = math.ceil(cores / 2)
        if cpr <= cores / 2:
            cpr += 1

        # last task gets only one core as competition is gone
        # (gpus will force spread)
        if len(tasks) == n_tasks - 1:
            cpr = 1

        tasks.append([ranks, cpr, gpr])
        cores -= cpr
        gpus  -= gpr

        # break on n_tasks tasks - use remaining cores / gpus for backfill
        if len(tasks) == n_tasks:
            break

    # backfill single node gpu tasks on all nodes
    while gpus:
        # can't schedule GPU tasks even if GPUs are free...
        if not cores:
            break
        for _ in range(n_nodes):
            tasks.append([1, 1, 1])
        gpus  -= 1
        cores -= 1

    # backfill single node cpu tasks on all nodes (at least 2 tasks)
    while cores:
        if cores > 2: cpt = random.randint(1, cores - 1)
        else        : cpt = 1
        for _ in range(n_nodes):
            tasks.append([1, cpt, 0])
        cores -= cpt

    for task in tasks:
        print(task)

    return tasks


def get_srun_commands(tasks):

    sruns = list()

    for task in tasks:
        ranks, cpr, gpr = task
        srun= f'srun -N {ranks} -n {ranks} ' \
              f'--ntasks-per-node=1 ' \
              f'--cores-per-task={cpr} ' \
              f'--gpus-per-task={gpr} sleep 60'
        sruns.append(srun)

    return sruns


if __name__ == "__main__":

    cpn     = int(sys.argv[1])
    gpn     = int(sys.argv[2])
    n_nodes = int(sys.argv[3])
    n_tasks = int(sys.argv[4])

    tasks = get_tasks(cpn, gpn, n_nodes, n_tasks)
    sruns = get_srun_commands(tasks)

    for srun in sruns:
        print(srun)

    sys.exit(0)



