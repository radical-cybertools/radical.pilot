#!/usr/bin/env python3

import os
import sys

import radical.utils as ru
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    cfg_file  = sys.argv[1]
    cfg_dir   = os.path.abspath(os.path.dirname(cfg_file))
    cfg_fname =                 os.path.basename(cfg_file)

    cfg       = ru.Config(cfg=ru.read_json(cfg_file))
    cpn       = cfg.cpn
    gpn       = cfg.gpn
    n_masters = cfg.n_masters
    n_workers = cfg.n_workers
    workload  = cfg.workload

    # each master uses a node, and each worker on each master uses a node
    nodes     =  n_masters + (n_masters * n_workers)
    print('nodes', nodes)

    master    = '%s/%s' % (cfg_dir, cfg.master)
    worker    = '%s/%s' % (cfg_dir, cfg.worker)

    master_sh = master.replace('py', 'sh')
    worker_sh = worker.replace('py', 'sh')

    session   = rp.Session()
    try:
        pd = rp.PilotDescription(cfg.pilot_descr)
        pd.cores   = nodes * cpn + cpn
        pd.gpus    = nodes * gpn + gpn
        pd.runtime = cfg.runtime

        tds = list()

        for i in range(n_masters):
            td = rp.TaskDescription(cfg.master_descr)
            td.uid            = ru.generate_id('master.%(item_counter)06d',
                                        ru.ID_CUSTOM,
                                        ns=session.uid)
            td.executable     = "/bin/sh"
            td.cpu_threads    = cpn
            td.gpu_processes  = gpn
            td.arguments      = [os.path.basename(master_sh), cfg_file, i]
            td.input_staging  = [{'source': master,
                                  'target': os.path.basename(master),
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': worker,
                                  'target': os.path.basename(worker),
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': master_sh,
                                  'target': os.path.basename(master_sh),
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': worker_sh,
                                  'target': os.path.basename(worker_sh),
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

        tmgr.add_pilots(pilot)
        tmgr.wait_tasks()

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

