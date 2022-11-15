#!/usr/bin/env python3


__copyright__ = "Copyright 2014-2015, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp


# DESCRIPTION: Tutorial 1: A Simple Workload consisting of a Bag-of-Tasks
#                             submitted to multiple machines
#

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
    session = rp.Session(name=session_name)
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
        c = rp.Context('ssh')
        c.user_id = "username"
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
        # If you want to run this example on XSEDE/ACCESS Gordon and Bridges2, you have
        # to add your allocation ID by setting the project attribute for each
        # pilot description ot it.
        #
        # A list of preconfigured resources can be found at:
        # https://radicalpilot.readthedocs.io/en/stable/ \
        #        machconf.html#preconfigured-resources
        #

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # The pilot_list will contain the description of the pilot that will be
        # submitted
        pilot_list = list()

        # Create the description of the first pilot and add it to the list
        pdesc = rp.PilotDescription ()
        pdesc.resource = "access.gordon"
        pdesc.runtime  = 10
        pdesc.cores    = 1
        pdesc.cleanup  = True
        pdesc.project  = ''
        pilot_list.append(pdesc)

        # Create the description of the secind pilot and add it to the list
        pdesc2 = rp.PilotDescription ()
        pdesc2.resource = "access.bridges2"
        pdesc2.runtime  = 10
        pdesc2.cores    = 1
        pdesc2.cleanup  = True
        pdesc2.project  = ''
        pilot_list.append(pdesc2)

        # Continue adding pilot by creating a new descrption and appending it to
        # the list.

        # Submit the pilot list to the Pilot Manager. Actually all the pilots are
        # submitted to the Pilot Manager at once.
        print("Submitting  Pilots to Pilot Manager ...")
        pilots = pmgr.submit_pilots(pilot_list)

        # Combine the Pilot, the Tasks and a scheduler via
        # a TaskManager object. The scheduler that supports multi-pilot sessions
        # is Round Robin. Direct Submittion does not.
        print("Initializing Task Manager ...")
        tmgr = rp.TaskManager (session=session,
                               scheduler=rp.SCHEDULER_ROUND_ROBIN)

        # Register our callback with the TaskManager. This callback will get
        # called every time any of the tasks managed by the TaskManager
        # change their state.
        tmgr.register_callback(task_state_cb)

        # Add the created Pilot to the TaskManager.
        print("Registering  Pilots with Task Manager ...")
        tmgr.add_pilots(pilots)

        NUMBER_JOBS  = 64  # the total number of tasks to run

        # submit tasks to pilot job
        taskdesc_list = []
        for i in range(NUMBER_JOBS):

            # -------- BEGIN USER DEFINED Task DESCRIPTION --------- #
            taskdesc = rp.TaskDescription()
            taskdesc.environment = {'task_NO': i}
            taskdesc.executable  = "/bin/echo"
            taskdesc.arguments   = ['I am Task number $task_NO from $HOSTNAME']
            taskdesc.cores       = 1
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

