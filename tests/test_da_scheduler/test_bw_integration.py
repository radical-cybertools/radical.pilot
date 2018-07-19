#!/usr/bin/env python

import os
import sys
import time

import radical.pilot as rp
import radical.utils as ru

# def test_da_scheduler_local_integration():

def test_bw_integration():


    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Define an [n]-core local pilot that runs for [x] minutes
    # Here we use a dict to initialize the description object
    pd_init = {'resource'      : 'ncsa.bw_aprun',
               'runtime'       : 10,  # pilot runtime (min)
               'cores'         : 128,
               'project'       : 'gk4',
               'queue'         : 'high'
              }
    pdesc = rp.ComputePilotDescription(pd_init)

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    # Register the ComputePilot in a UnitManager object.
    umgr = rp.UnitManager(session=session)
    umgr.add_pilots(pilot)

    # Run 16 tasks that each require 1 core and 10MB of LFS
    n = 4  
    cuds = list()
    for i in range(0, n):

        # create a new CU description, and fill it.
        # Here we don't use dict initialization.
        cud = rp.ComputeUnitDescription()
        cud.executable       = '/bin/hostname'
        cud.arguments = ['>', 's1_t%s_hostname.txt' % i]
        cud.cpu_processes    = 1
        cud.cpu_threads      = 16
        # cud.cpu_process_type = None
        # cud.cpu_process_type = rp.MPI
        cud.lfs_per_process  = 10   # MB
        cud.output_staging = {'source': 'unit:///s1_t%s_hostname.txt' % i,
                              'target': 'client:///s1_t%s_hostname.txt' % i,
                              'action': rp.TRANSFER}
        cuds.append(cud)


    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    cus = umgr.submit_units(cuds)

    # Wait for all units to finish
    umgr.wait_units()

    n = 4
    cuds2 = list()
    for i in range(0, n):

        # create a new CU description, and fill it.
        # Here we don't use dict initialization.
        cud = rp.ComputeUnitDescription()
        cud.tag = cus[i].uid
        cud.executable       = '/bin/hostname'
        cud.arguments = ['>', 's2_t%s_hostname.txt' % i]
        cud.cpu_processes    = 1
        cud.cpu_threads      = 16
        cud.cpu_process_type = None
        # cud.cpu_process_type = rp.MPI
        cud.lfs_per_process  = 10   # MB
        cud.output_staging = {'source': 'unit:///s2_t%s_hostname.txt' % i,
                              'target': 'client:///s2_t%s_hostname.txt' % i,
                              'action': rp.TRANSFER}
        cuds2.append(cud)


    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    cus2 = umgr.submit_units(cuds2)


    # Wait for all units to finish
    umgr.wait_units()

    # Check that all units succeeded
    for i in range(0, n):
        assert open('s1_t%s_hostname.txt'%i,'r').readline().strip() == open('s2_t%s_hostname.txt'%i,'r').readline().strip()

    session.close() 


# ------------------------------------------------------------------------------

