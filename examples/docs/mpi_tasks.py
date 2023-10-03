#!/usr/bin/env python3

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import os
import radical.pilot as rp


""" DESCRIPTION: Tutorial 4: A workload consisting of of MPI tasks
"""

# READ: The RADICAL-Pilot documentation:
#   https://radicalpilot.readthedocs.io/en/stable/
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if
# you want to see what happens behind the scenes!


# ------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print("[Callback]: Pilot '%s' state: %s." % (pilot.uid, state))

    if state == rp.FAILED:
        sys.exit (1)


# ------------------------------------------------------------------------------
#
def task_state_cb (task, state):

    if not task:
        return

    global CNT

    print("[Callback]: task %s on %s: %s." % (task.uid, task.pilot_id, state))

    if state == rp.FAILED:
        print("stderr: %s" % task.stderr)
        sys.exit(2)


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # we can optionally pass session name to RP
    if len(sys.argv) > 1:
        session_name = sys.argv[1]
    else:
        session_name = None

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session(name=session_name,
                         dburl=os.environ.get('RADICAL_PILOT_DBURL'))
    print("session id: %s" % session.uid)

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        #
        # Change the user name below if you are using a remote resource
        # and your username on that resource is different from the username
        # on your local machine.
        #
        c = rp.Context('userpass')
      # c.user_id = "tutorial_X"
      # c.user_pass = "PutYourPasswordHere"
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more Pilots.
        print("Initializing Pilot Manager ...")
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        #
        # Change the resource below if you want to run on a another resource than
        # the pre-configured tutorial cluster.
        # You also might have to set the 'project' to your allocation ID if
        # your remote resource requires so.
        #
        # A list of pre-configured resources can be found at:
        # https://radicalpilot.readthedocs.io/en/stable/ \
        #        machconf.html#preconfigured-resources
        #
        pdesc = rp.PilotDescription ()
        pdesc.resource = "local.localhost"
        pdesc.runtime  = 10
        pdesc.cores    = 16
        pdesc.cleanup  = True

        # submit the pilot.
        print("Submitting  Pilot to Pilot Manager ...")
        pilot = pmgr.submit_pilots(pdesc)

        # Combine the Pilot, the Tasks and a scheduler via
        # a TaskManager object.
        print("Initializing Task Manager ...")
        tmgr = rp.TaskManager (session=session,
                               scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

        # Register our callback with the TaskManager. This callback will get
        # called every time any of the tasks managed by the TaskManager
        # change their state.
        tmgr.register_callback(task_state_cb)

        # Add the created Pilot to the TaskManager.
        print("Registering  Pilot with Task Manager ...")
        tmgr.add_pilots(pilot)

        NUMBER_JOBS  = 10  # the total number of tasks to run

        # submit tasks to pilot job
        taskdesc_list = []
        for i in range(NUMBER_JOBS):

            # -------- BEGIN USER DEFINED Task DESCRIPTION --------- #
            taskdesc = rp.TaskDescription()
            taskdesc.executable    = "python"
            taskdesc.arguments     = ["helloworld_mpi.py"]
            taskdesc.input_staging = ["../helloworld_mpi.py"]
            taskdesc.cores         = 8
            taskdesc.mpi           = True
            # -------- END USER DEFINED Task DESCRIPTION --------- #

            taskdesc_list.append(taskdesc)

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        print("Submit Tasks to Task Manager ...")
        task_set = tmgr.submit_tasks (taskdesc_list)

        print("Waiting for tasks to complete ...")
        tmgr.wait_tasks()
        print("All tasks completed successfully!")

        for task in task_set:
            print('* Task %s - state: %s, exit code: %s, started: %s, '
                  'finished: %s, stdout: %s' % (task.uid, task.state,
                  task.exit_code, task.start_time, task.stop_time, task.stdout))

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
        print("closing session")
        session.close ()

        # the above is equivalent to
        #
        #   session.close (terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


# ------------------------------------------------------------------------------

