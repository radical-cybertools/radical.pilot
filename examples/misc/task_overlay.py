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

    runtime   = 60
    resource  = 'local.debug'

    cfg       = ru.Config(cfg=ru.read_json(cfg_file))
    nodes     = cfg.nodes
    cpn       = cfg.cpn
    gpn       = cfg.gpn

    master    = '%s/%s' % (cfg_dir, cfg.master)
    worker    = '%s/%s' % (cfg_dir, cfg.worker)

    session   = rp.Session()
    try:
        pd = rp.ComputePilotDescription(cfg.pd)
        pd.cores   = nodes * cpn
        pd.gpus    = nodes * gpn
        pd.runtime = runtime

        td = rp.ComputeUnitDescription(cfg.td)
        td.arguments     = [master, cfg_fname]
        td.input_staging = [master, worker, cfg_file]

        pmgr  = rp.PilotManager(session=session)
        umgr  = rp.UnitManager(session=session)
        pilot = pmgr.submit_pilots(pd)
        task  = umgr.submit_units(td)

        umgr.add_pilots(pilot)
        umgr.wait_units()

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

