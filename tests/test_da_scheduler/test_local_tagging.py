#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__ = 'MIT'

import os
import sys
import time
from glob import glob

import radical.pilot as rp
import radical.utils as ru

dh = ru.DebugHelper()

# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
def test_local_tagging():

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Define an [n]-core local pilot that runs for [x] minutes
    # Here we use a dict to initialize the description object
    pd_init = {'resource': 'local.localhost',
               'runtime': 10,  # pilot runtime (min)
               'exit_on_error': True,
               'cores': 4
               }
    pdesc = rp.ComputePilotDescription(pd_init)

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    report.header('submit units')

    # Register the ComputePilot in a UnitManager object.
    umgr = rp.UnitManager(session=session)
    umgr.add_pilots(pilot)

    # Create a workload of ComputeUnits.
    # Each compute unit runs '/bin/date'.

    n = 5  # number of units to run
    report.info('create %d unit description(s)\n\t' % n)

    cuds = list()
    for i in range(0, n):

        # create a new CU description, and fill it.
        # Here we don't use dict initialization.
        cud = rp.ComputeUnitDescription()
        cud.executable = '/bin/hostname'
        cud.arguments = ['>', 's1_t%s_hostname.txt' % i]
        cud.cpu_processes = 1
        cud.cpu_threads = 1
        #cud.cpu_process_type = rp.MPI
        #cud.cpu_thread_type  = rp.OpenMP
        cud.output_staging = {'source': 'unit:///s1_t%s_hostname.txt' % i,
                              'target': 'client:///s1_t%s_hostname.txt' % i,
                              'action': rp.TRANSFER}
        cuds.append(cud)
        report.progress()
    report.ok('>>ok\n')

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    cus = umgr.submit_units(cuds)

    # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
    report.header('gather results')
    umgr.wait_units()

    n = 5  # number of units to run
    report.info('create %d unit description(s)\n\t' % n)

    cuds = list()
    for i in range(0, n):

        # create a new CU description, and fill it.
        # Here we don't use dict initialization.
        cud = rp.ComputeUnitDescription()
        cud.executable = '/bin/hostname'
        cud.arguments = ['>', 's2_t%s_hostname.txt' % i]
        cud.cpu_processes = 1
        cud.cpu_threads = 1
        cud.tag = cus[i].uid
        #cud.cpu_process_type = rp.MPI
        #cud.cpu_thread_type  = rp.OpenMP
        cud.output_staging = {'source': 'unit:///s2_t%s_hostname.txt' % i,
                              'target': 'client:///s2_t%s_hostname.txt' % i,
                              'action': rp.TRANSFER}
        cuds.append(cud)
        report.progress()
    report.ok('>>ok\n')

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    cus = umgr.submit_units(cuds)

    # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
    report.header('gather results')
    umgr.wait_units()

    for i in range(0, n):
        assert open('s1_t%s_hostname.txt'%i,'r').readline().strip() == open('s2_t%s_hostname.txt'%i,'r').readline().strip()

    report.header('finalize')
    session.close(download=True)

    report.header()

    txts = glob('%s/*.txt' % os.getcwd())
    for f in txts:
        os.remove(f)


# ------------------------------------------------------------------------------
