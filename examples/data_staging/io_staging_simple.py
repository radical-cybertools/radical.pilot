#!/usr/bin/env python3

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp

# READ: The RADICAL-Pilot documentation:
#   https://radicalpilot.readthedocs.io/en/stable/
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if
# you want to see what happens behind the scenes!


# ------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):
    """ this callback is invoked on all pilot state changes """

    print("[Callback]: Pilot '%s' state: %s." % (pilot.uid, state))

    if state == rp.FAILED:
        sys.exit (1)


# ------------------------------------------------------------------------------
#
def task_state_cb (task, state):
    """ this callback is invoked on all task state changes """

    print("[Callback]: Task '%s' state: %s." % (task.uid, state))

    if state == rp.FAILED:
        sys.exit (1)


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
    session = rp.Session(uid=session_name)
    print("session id: %s" % session.uid)

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # Add a Pilot Manager. Pilot managers manage one or more Pilots.
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a single-core local pilot that runs for 5 minutes and cleans up
        # after itself.
        pdesc = rp.PilotDescription()
        pdesc.resource = "local.localhost"
        pdesc.cores    = 1
        pdesc.runtime  = 5

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Create a Taskt that sorts the local password file and writes the
        # output to result.dat.
        #
        #  The exact command that is executed by the agent is:
        #    "/usr/bin/sort -o result.dat passwd"
        #
        td = rp.TaskDescription()
        td.executable     = "/usr/bin/sort"
        td.arguments      = ["-o", "result.dat", "passwd"]
        td.input_staging  = "/etc/passwd"
        td.output_staging = "result.dat"

        # Combine the Pilot, the Tasks and a scheduler via
        # a TaskManager object.
        tmgr = rp.TaskManager(session=session)

        # Register our callback with the TaskManager. This callback will get
        # called every time any of the tasks managed by the TaskManager
        # change their state.
        tmgr.register_callback(task_state_cb)

        # Add the previously created Pilot to the TaskManager.
        tmgr.add_pilots(pilot)

        # Submit the previously created Task description to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning the Task to the Pilot.
        task = tmgr.submit_tasks(td)

        # Wait for the task to reach a terminal state (DONE or FAILED).
        tmgr.wait_tasks()

        print("* Task %s state: %s, exit code: %s,"
              % (task.uid, task.state, task.exit_code))

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
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


# -------------------------------------------------------------------------------

