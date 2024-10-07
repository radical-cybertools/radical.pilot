#!/usr/bin/env python3

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: https://radicalpilot.readthedocs.io/
#
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Failing Tasks (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    resource = None
    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: resource = sys.argv[1]
    else                   : resource = 'local.localhost'


    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyway...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # read the config used for resource details
        config = ru.read_json('%s/config.json' %
                              os.path.dirname(__file__)).get(resource, {})
        pmgr   = rp.PilotManager(session=session)
        tmgr   = rp.TaskManager(session=session)

        report.header('submit pilots')

        # Add a PilotManager. PilotManagers manage one or more pilots.

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pd_init = {'resource'      : resource,
                   'runtime'       : 15,  # pilot runtime (min)
                   'exit_on_error' : True,
                   'project'       : config.get('project'),
                   'queue'         : config.get('queue'),
                   'access_schema' : config.get('schema'),
                   'cores'         : config.get('cores', 1),
                   'gpus'          : config.get('gpus',  0)
                  }
        pdesc = rp.PilotDescription(pd_init)

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        n = 128   # number of tasks to run
        report.header('submit %d tasks' % n)

        # Register the pilot in a TaskManager object.
        tmgr.add_pilots(pilot)

        # Create a workload of tasks.
        # Each task runs '/bin/date'.

        report.progress_tgt(n, label='create')
        tds = list()
        for i in range(n):

            # create a new task description, and fill it.
            td = rp.TaskDescription()
            if i % 10:
                td.executable = '/bin/date'
            else:
                # trigger an error now and then
                td.executable = '/bin/data'  # does not exist
            tds.append(td)
            report.progress()

        report.progress_done()

        # Submit the previously created task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning tasks to the pilots.
        tasks = tmgr.submit_tasks(tds)

        # Wait for all tasks to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')
        tmgr.wait_tasks()

        report.info('\n')
        for task in tasks:
            if task.state in [rp.FAILED, rp.CANCELED]:
                report.plain('  * %s: %s, exit: %5s, err: %35s'
                            % (task.uid, task.state[:4],
                               task.exit_code, task.stderr))
                report.error('>>err\n')

            else:
                report.plain('  * %s: %s, exit: %5s, out: %35s'
                            % (task.uid, task.state[:4],
                               task.exit_code, task.stdout))
                report.ok('>>ok\n')


    except Exception as e:
        # Something unexpected happened in the pilot code above
        report.error('caught Exception: %s\n' % e)
        ru.print_exception_trace()
        raise

    except (KeyboardInterrupt, SystemExit):
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        ru.print_exception_trace()
        report.warn('exit requested\n')

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        session.close(download=True)

    report.header()


# ------------------------------------------------------------------------------

