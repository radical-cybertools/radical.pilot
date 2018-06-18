#!/usr/bin/env python

import os
import sys
import time

import radical.pilot as rp
import radical.utils as ru

# def test_da_scheduler_local_integration():

if __name__ == '__main__':


    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    cfg = session.get_resource_config('local.localhost')
    new_cfg = rp.ResourceConfig('local.localhost', cfg)
    new_cfg.lfs_path_per_node = '/tmp'
    new_cfg.lfs_size_per_node = 1024 # MB
    session.add_resource_config(new_cfg)

    # Define an [n]-core local pilot that runs for [x] minutes
    # Here we use a dict to initialize the description object
    pd_init = {'resource'      : 'local.localhost',
               'runtime'       : 15,  # pilot runtime (min)
               'cores'         : 4
              }
    pdesc = rp.ComputePilotDescription(pd_init)

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    # Register the ComputePilot in a UnitManager object.
    umgr = rp.UnitManager(session=session)
    umgr.add_pilots(pilot)

    # Create a workload of ComputeUnits.
    # Each compute unit runs '/bin/date'.

    n = 16  # number of units to run

    cuds = list()
    for i in range(0, n):

        # create a new CU description, and fill it.
        # Here we don't use dict initialization.
        cud = rp.ComputeUnitDescription()
        cud.executable       = '/bin/hostname'
        cud.cpu_processes    = 1
        cud.cpu_threads      = 1
        # cud.cpu_process_type = rp.MPI
        cud.lfs_per_process  = 10   # MB
        cuds.append(cud)


    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    umgr.submit_units(cuds)

    umgr.wait_units()

    session.close(download=True)
# ------------------------------------------------------------------------------

