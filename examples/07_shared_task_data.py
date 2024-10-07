#!/usr/bin/env python3

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

verbose  = os.environ.get('RADICAL_PILOT_VERBOSE', 'REPORT')
os.environ['RADICAL_PILOT_VERBOSE'] = verbose

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: https://radicalpilot.readthedocs.io/
#
# ------------------------------------------------------------------------------


# -----------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Shared Task Data (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: resource = sys.argv[1]
    else                   : resource = 'local.localhost'

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # read the config used for resource details
        report.info('read config')
        config = ru.read_json('%s/config.json' % os.path.dirname(os.path.abspath(__file__)))
        report.ok('>>ok\n')

        report.header('submit pilots')

        # Add a Pilot Manager. Pilot managers manage one or more Pilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pd_init = {
                   'resource'      : resource,
                   'runtime'       : 15,  # pilot runtime (min)
                   'exit_on_error' : True,
                   'project'       : config[resource].get('project', None),
                   'queue'         : config[resource].get('queue', None),
                   'access_schema' : config[resource].get('schema', None),
                   'cores'         : config[resource].get('cores', 1),
                   'gpus'          : config[resource].get('gpus', 0),
                  }
        pdesc = rp.PilotDescription(pd_init)

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Create a workload of char-counting a simple file.  We first create the
        # file right here, and stage it to the pilot 'shared_data' space
        os.system('hostname >  input.dat')
        os.system('date     >> input.dat')

        # Synchronously stage the data to the pilot
        report.info('stage in shared data')
        pilot.stage_in({'source': 'client:///input.dat',
                        'target': 'pilot:///input.dat',
                        'action': rp.TRANSFER})
        report.ok('>>ok\n')


        report.header('submit tasks')

        # Register the Pilot in a TaskManager object.
        tmgr = rp.TaskManager(session=session)
        tmgr.add_pilots(pilot)

        n = 128   # number of tasks to run
        report.info('create %d task description(s)\n\t' % n)

        tds = list()
        outs = list()
        for i in range(0, n):

            # create a new Task description, and fill it.
            # Here we don't use dict initialization.
            td = rp.TaskDescription()
            td.executable     = '/bin/cat'
            td.arguments      = ['input.dat']
            td.stdout         = 'STDOUT'
            td.input_staging  = {'source': 'pilot:///input.dat',
                                 'target': 'task:///input.dat',
                                 'action': rp.LINK}
            td.output_staging = {'source': 'task:///STDOUT',
                                 'target': 'pilot:///STDOUT.%06d' % i,
                                 'action': rp.COPY}
            outs.append('STDOUT.%06d' % i)
            tds.append(td)
            report.progress()
        report.ok('>>ok\n')

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        tasks = tmgr.submit_tasks(tds)

        # Wait for all tasks to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')
        tmgr.wait_tasks()

        report.info('\n')
        for task in tasks:
            report.plain('  * %s: %s, exit: %3s, out: %s\n'
                    % (task.uid, task.state[:4],
                        task.exit_code, task.stdout.strip()[:35]))

        # delete the sample input files
        os.system('rm input.dat')

        # Synchronously stage the data to the pilot
        report.info('stage out shared data')
        pilot.stage_out([{'source': 'pilot:///%s'  % fname,
                          'target': 'client:///%s' % fname,
                          'action': rp.TRANSFER} for fname in outs])
        report.ok('>>ok\n')


    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        session.close()

    report.header()


# ------------------------------------------------------------------------------

