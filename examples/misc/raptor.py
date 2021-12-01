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
    cpn       = cfg.cpn
    gpn       = cfg.gpn
    n_masters = cfg.n_masters
    n_workers = cfg.n_workers
    workload  = cfg.workload

    # each master uses a node, and each worker on each master uses a node
    nodes     =  n_masters + (n_masters * n_workers)
    print('nodes', nodes)

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
            td.cpu_threads    = cpn
            td.gpu_processes  = gpn
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



        requests = list()
        for i in range(eval(cfg.workload.total)):

            td  = rp.TaskDescription()
            uid = 'request.req.%06d' % i
            # ------------------------------------------------------------------
            # work serialization goes here
            work = json.dumps({'mode'   :  'call',
                               'cores'  :  1,
                               'timeout':  100,
                               'data'   : {'method': 'hello',
                                           'kwargs': {'count': i,
                                                      'uid'  : uid}}})
            # ------------------------------------------------------------------
            requests.append(rp.TaskDescription({
                               'uid'       : uid,
                               'executable': '-',
                               'scheduler' : 'master.%06d' % (i % n_masters),
                               'arguments' : [work]}))

        tmgr.submit_tasks(requests)

        tmgr.add_pilots(pilot)
        tmgr.wait_tasks(uids=[r['uid'] for r in requests])

        time.sleep(120)

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

