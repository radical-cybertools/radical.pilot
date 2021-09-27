#!/usr/bin/env python3

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import radical.pilot as rp
import radical.utils as ru

verbose  = os.environ.get('RADICAL_PILOT_VERBOSE', 'REPORT')
os.environ['RADICAL_PILOT_VERBOSE'] = verbose

""" DESCRIPTION: Tutorial 1: A Simple Workload consisting of a Bag-of-Tasks
"""

# READ: The RADICAL-Pilot documentation:
#   https://radicalpilot.readthedocs.io/en/stable/
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if
# you want to see what happens behind the scenes!


#
if __name__ == "__main__":

    RESOURCE_LABEL = 'local.localhost'
    PILOT_CORES    =  2
    BAG_SIZE       = 10  # The number of tasks
    TASK_CORES     =  1  # The cores each Task will take.
    QUEUE          = None
    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' in the 'finally' clause.
    try:

        # Add a Pilot Manager. Pilot managers manage one or more Pilots.
        pmgr = rp.PilotManager(session=session)

        #
        # Change the resource below if you want to run on a remote resource.
        # You also might have to set the 'project' to your allocation ID if
        # your remote resource does compute time accounting.
        #
        # A list of preconfigured resources can be found at:
        # https://radicalpilot.readthedocs.io/en/stable/ \
        #        machconf.html#preconfigured-resources
        #
        pdesc = rp.PilotDescription ()
        pdesc.resource = RESOURCE_LABEL  # this is a "label", not a hostname
        pdesc.cores    =  PILOT_CORES
        pdesc.runtime  = 30    # minutes
        pdesc.cleanup  = True  # clean pilot sandbox and database entries
        pdesc.queue = QUEUE

        # submit the pilot.
        report.header("Submitting  Pilot to Pilot Manager ...")
        pilot = pmgr.submit_pilots(pdesc)

        # create a TaskManager which schedules Tasks over pilots.
        report.header("Initializing Task Manager ...")
        tmgr = rp.TaskManager (session=session)


        # Add the created Pilot to the TaskManager.
        report.ok('>>ok\n')

        tmgr.add_pilots(pilot)

        report.info('Create %d Task Description(s)\n\t' % BAG_SIZE)

        # create Task descriptions
        taskdesc_list = []
        for i in range(BAG_SIZE):

            # -------- BEGIN USER DEFINED Task DESCRIPTION --------- #
            taskdesc = rp.TaskDescription()
            taskdesc.executable  = "/bin/echo"
            taskdesc.arguments   = ['I am Task number $TASK_NO']
            taskdesc.environment = {'TASK_NO': i}
            taskdesc.cores       = 1
            # -------- END USER DEFINED Task DESCRIPTION --------- #

            taskdesc_list.append(taskdesc)
            report.progress()
        report.ok('>>>ok\n')
        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        report.header("Submit Tasks to Task Manager ...")
        task_set = tmgr.submit_tasks (taskdesc_list)

        report.header("Waiting for tasks to complete ...")
        tmgr.wait_tasks()

        for task in task_set:
            print("* Task %s, state %s, exit code: %s, stdout: %s"
                % (task.uid, task.state, task.exit_code, task.stdout.strip()))


    except Exception as e:
        # Something unexpected happened in the pilot code above
        print("caught Exception: %s" % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print("need to exit now: %s" % e)

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        report.header("closing session")
        session.close ()

        # the above is equivalent to
        #
        #   session.close (terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots.
    report.header()


# ------------------------------------------------------------------------------

