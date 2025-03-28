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


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Task Runtime Environment (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: resource = sys.argv[1]
    else                   : resource = 'local.localhost'

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted. If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # read the config used for resource details
        report.info('read config')
        config = ru.read_json('%s/config.json' % os.path.dirname(__file__))
        report.ok('>>ok\n')

        report.header('submit pilots')

        # Add a PilotManager. PilotManagers manage one or more pilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pd_init = {'resource'      : resource,
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

        report.header('prepare task env')
        pilot.prepare_env('numpy_env', {'type'    : 'virtualenv',
                                        'setup'   : ['numpy'],
                                        'pre_exec': ['export FOO=bar',
                                                     'export BAR=$FOO',
                                                     'echo $BAR']})
        report.ok('ok')

        report.header('submit tasks')

        # Register the pilot in a TaskManager object.
        tmgr = rp.TaskManager(session=session)
        tmgr.add_pilots(pilot)

        # Create a workload of tasks.
        # Each task runs '/bin/date'.
        n = 2  # number of tasks to run
        report.info('create %d task description(s)\n\t' % n)

        tds = list()
        for i in range(0, n):

            # create a new task description, and fill it.
            # Here we don't use dict initialization.
            td = rp.TaskDescription()
            td.executable = 'python3'
            td.arguments  = ['-c', 'import numpy; print(numpy.__file__)']
            td.named_env  = 'numpy_env'
            tds.append(td)
            report.progress()

        report.ok('>>ok\n')

        # Submit the previously created task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning tasks to the pilots.
        tasks = tmgr.submit_tasks(tds)

        # Wait for all tasks to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')
        tmgr.wait_tasks()

        report.info('\n')
        for task in tasks:
            report.plain('  * %s: %s, exit: %3s, out: %s\n'
                    % (task.uid, task.state[:4],
                        task.exit_code, task.stdout[:35]))

        # get some more details for one task:
        task_dict = tasks[0].as_dict()
        report.plain("task workdir : %s\n" % task_dict['task_sandbox'])
        report.plain("pilot id     : %s\n" % task_dict['pilot'])
        report.plain("exit code    : %s\n" % task_dict['exit_code'])
        report.plain("stdout       : %s\n" % task_dict['stdout'])

        # get some more details for one task:
        task_dict = tasks[1].as_dict()
        report.plain("task workdir : %s\n" % task_dict['task_sandbox'])
        report.plain("pilot id     : %s\n" % task_dict['pilot'])
        report.plain("exit code    : %s\n" % task_dict['exit_code'])
        report.plain("exit stdout  : %s\n" % task_dict['stdout'])


    except Exception as e:
        # Something unexpected happened in the pilot code above
        report.error('caught Exception: %s\n' % e)
        raise

    except (KeyboardInterrupt, SystemExit):
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        report.warn('exit requested\n')

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        session.close()

    report.header()


# ------------------------------------------------------------------------------

