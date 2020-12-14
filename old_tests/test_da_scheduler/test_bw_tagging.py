#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__ = 'MIT'

import os
import glob

import radical.pilot as rp
import radical.utils as ru

# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: https://radicalpilot.readthedocs.io/
#
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
def test_bw_tagging():

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # Add a Pilot Manager. Pilot managers manage one or more Pilots.
    pmgr = rp.PilotManager(session=session)

    # Define an [n]-core local pilot that runs for [x] minutes
    # Here we use a dict to initialize the description object
    pd_init = {'resource': 'ncsa.bw_aprun',
               'runtime': 10,  # pilot runtime (min)
               'exit_on_error': True,
               'project': 'gk4',
               'queue': 'high',
               'access_schema': 'gsissh',
               'cores': 128
              }
    pdesc = rp.PilotDescription(pd_init)

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    report.header('submit tasks')

    # Register the Pilot in a TaskManager object.
    tmgr = rp.TaskManager(session=session)
    tmgr.add_pilots(pilot)

    # Create a workload of Tasks.
    # Each task runs '/bin/date'.

    n = 5  # number of tasks to run
    report.info('create %d task description(s)\n\t' % n)

    tds = list()
    for i in range(0, n):

        # create a new Task description, and fill it.
        # Here we don't use dict initialization.
        td                  = rp.TaskDescription()
        td.executable       = '/bin/hostname'
        td.arguments        = ['>', 's1_t%s_hostname.txt' % i]
        td.cpu_processes    = 1
        td.cpu_threads      = 16
      # td.cpu_process_type = rp.MPI
      # td.cpu_thread_type  = rp.OpenMP
        td.output_staging   = {'source': 'task:///s1_t%s_hostname.txt' % i,
                                'target': 'client:///s1_t%s_hostname.txt' % i,
                                'action': rp.TRANSFER}
        tds.append(td)
        report.progress()
    report.ok('>>ok\n')

    # Submit the previously created Task descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning Tasks to the Pilots.
    cus = tmgr.submit_tasks(tds)

    # Wait for all tasks to reach a final state
    # (DONE, CANCELED or FAILED).
    report.header('gather results')
    tmgr.wait_tasks()

    n = 5  # number of tasks to run
    report.info('create %d task description(s)\n\t' % n)

    tds = list()
    for i in range(0, n):

        # create a new Task description, and fill it.
        # Here we don't use dict initialization.
        td                  = rp.TaskDescription()
        td.executable       = '/bin/hostname'
        td.arguments        = ['>', 's2_t%s_hostname.txt' % i]
        td.cpu_processes    = 1
        td.cpu_threads      = 16
        td.tag              = cus[i].uid
      # td.cpu_process_type = rp.MPI
      # td.cpu_thread_type  = rp.OpenMP
        td.output_staging   = {'source': 'task:///s2_t%s_hostname.txt' % i,
                                'target': 'client:///s2_t%s_hostname.txt' % i,
                                'action': rp.TRANSFER}
        tds.append(td)
        report.progress()
    report.ok('>>ok\n')

    # Submit the previously created Task descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning Tasks to the Pilots.
    cus = tmgr.submit_tasks(tds)

    # Wait for all tasks to reach a final state (DONE, CANCELED or FAILED).
    report.header('gather results')
    tmgr.wait_tasks()

    for i in range(0, n):
        assert open('s1_t%s_hostname.txt' % i,'r').readline().strip() == \
               open('s2_t%s_hostname.txt' % i,'r').readline().strip()

    report.header('finalize')
    session.close(download=True)

    report.header()

    for f in glob.glob('%s/*.txt' % os.getcwd()):
        os.remove(f)


# ------------------------------------------------------------------------------

