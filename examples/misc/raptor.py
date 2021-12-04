#!/usr/bin/env python3

import os
import sys
import json
import time

import radical.utils as ru
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    cfg_file  = sys.argv[1]
    cfg_dir   = os.path.abspath(os.path.dirname(cfg_file))
    cfg_fname =                 os.path.basename(cfg_file)

    cfg       = ru.Config(cfg=ru.read_json(cfg_file))
    cpn       = cfg.worker_descr.cpu_processes or 1
    gpn       = cfg.worker_descr.gpu_processes or 0
    n_masters = cfg.n_masters
    n_workers = cfg.n_workers
    workload  = cfg.workload

    # each master uses a node, and each worker on each master uses a node
    # use 8 additional cores for non-raptor tasks
    session   = rp.Session()
    try:
        pd = rp.PilotDescription(cfg.pilot_descr)
        pd.cores   = n_masters + n_workers * cpn + 8
        pd.gpus    =             n_workers * gpn
        pd.runtime = cfg.runtime

        tds = list()

        for i in range(n_masters):
            td = rp.TaskDescription(cfg.master_descr)
            td.uid            = ru.generate_id('master.%(item_counter)06d',
                                               ru.ID_CUSTOM,
                                               ns=session.uid)
            td.arguments      = [cfg_file, i]
            td.input_staging  = [{'source': 'raptor_master.py',
                                  'target': 'raptor_master.py',
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': 'raptor_worker.py',
                                  'target': 'raptor_worker.py',
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': cfg_file,
                                  'target': os.path.basename(cfg_file),
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS}
                                ]
            tds.append(td)

        pmgr  = rp.PilotManager(session=session)
        tmgr  = rp.TaskManager(session=session)
        pilot = pmgr.submit_pilots(pd)
        task  = tmgr.submit_tasks(tds)
        pilot.prepare_env(env_name='ve_raptor',
                          env_spec={'type'   : 'virtualenv',
                                    'version': '3.8',
                                    'setup'  : ['radical.pilot']})

        tds = list()
        for i in range(eval(cfg.workload.total)):

            tds.append(rp.TaskDescription({
                'uid'             : 'task.exe.%06d' % i,
                'mode'            : rp.RP_EXECUTABLE,
                'cpu_processes'   : 4,
                'cpu_process_type': rp.MPI,
                'executable'      : '/bin/sh',
                'arguments'       : ['-c', 'echo "hello $RP_RANK/$RP_RANKS: '
                                           '$RP_TASK_ID"']}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.mpi.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.RP_FUNCTION,
                'cpu_processes'   : 4,
                'cpu_process_type': rp.MPI,
                'function'        : 'test_mpi',
                'kwargs'          : {'msg': 'task.mpi.%06d' % i},
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.eval.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.RP_EVAL,
                'cpu_processes'   : 4,
                'cpu_process_type': rp.MPI,
                'code'            :
                    'print("hello %s/%s: %s" % (os.environ["RP_RANK"],'
                    'os.environ["RP_RANKS"], os.environ["RP_TASK_ID"]))',
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.exec.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.RP_EXEC,
                'cpu_processes'   : 4,
                'cpu_process_type': rp.MPI,
                'code'            :
                    'import os\nprint("hello %s/%s: %s" % (os.environ["RP_RANK"],'
                    'os.environ["RP_RANKS"], os.environ["RP_TASK_ID"]))',
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.proc.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.RP_PROC,
                'cpu_processes'   : 4,
                'cpu_process_type': rp.MPI,
                'executable'      : '/bin/sh',
                'arguments'       : ['-c', 'echo "hello $RP_RANK/$RP_RANKS: '
                                           '$RP_TASK_ID"'],
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.shell.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.RP_SHELL,
                'cpu_processes'   : 4,
                'cpu_process_type': rp.MPI,
                'command'         : 'echo "hello $RP_RANK/$RP_RANKS: $RP_TASK_ID"',
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

        tasks = tmgr.submit_tasks(tds)

        tmgr.add_pilots(pilot)
        tmgr.wait_tasks(uids=[t.uid for t in tasks])

        for task in tasks:
            print('%s : %s : %s' % (task.uid, task.stdout, task.stderr))

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

