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
    nodes     = cfg.nodes
    cpn       = cfg.cpn
    gpn       = cfg.gpn
    n_masters = cfg.n_masters
    workload  = cfg.workload

    master    = '%s/%s' % (cfg_dir, cfg.master)
    worker    = '%s/%s' % (cfg_dir, cfg.worker)

    session   = rp.Session()
    try:
        pd = rp.ComputePilotDescription(cfg.pilot_descr)
        pd.cores   = nodes * cpn
        pd.gpus    = nodes * gpn
        pd.runtime = cfg.runtime

        tds = list()

        for i in range(n_masters):
            td = rp.ComputeUnitDescription(cfg.master_descr)
            td.executable     = "python3"
            td.arguments      = [os.path.basename(master), cfg_file, i]
            td.input_staging  = [{'source': master,
                                  'target': os.path.basename(master),
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': worker,
                                  'target': os.path.basename(worker),
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': cfg_file,
                                  'target': os.path.basename(cfg_file),
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS}
                                ]
            tds.append(td)

        pmgr  = rp.PilotManager(session=session)
        umgr  = rp.UnitManager(session=session)
        pilot = pmgr.submit_pilots(pd)
        task  = umgr.submit_units(tds)

        umgr.add_pilots(pilot)
        umgr.wait_units()

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

