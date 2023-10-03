#!/usr/bin/env python3

# pyxlint: disable=redefined-outer-name
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp

# ATTENTION:
#
# This example needs significant time to run, and there is some probability that
# the larger futuregrid pilots are not getting through the batch queue at all.
# It is thus not part of the RP test suite.

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

    print("[Callback]: Task  '%s' state: %s." % (task.uid, state))

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

        pdesc = rp.PilotDescription()
        pdesc.resource  = "local.localhost"
        pdesc.runtime   = 12  # minutes
        pdesc.cores     = 4
        pdesc.cleanup   = True

        pilot_1 = pmgr.submit_pilots(pdesc)

        # create a second pilot with a new description
        pdesc = rp.PilotDescription()
        pdesc.resource  = "local.localhost"
        pdesc.runtime   = 40  # minutes
        pdesc.cores     = 32

        pilot_2 = pmgr.submit_pilots(pdesc)

        # reuse the pilot description for the third pilot
        pdesc.cores     = 128

        pilot_3 = pmgr.submit_pilots(pdesc)

        # Combine the Pilot, the Tasks and a scheduler via
        # a TaskManager object.
        tmgr = rp.TaskManager (session=session, scheduler=rp.SCHEDULER_BACKFILLING)

        # Register our callback with the TaskManager. This callback will get
        # called every time any of the tasks managed by the TaskManager
        # change their state.
        tmgr.register_callback(task_state_cb)

        # Add the previsouly created Pilot to the TaskManager.
        tmgr.add_pilots([pilot_1, pilot_2, pilot_3])

      # # wait until first pilots become active
        pilot_1.wait (state=rp.PMGR_ACTIVE)

        # Create a workload of 8 Tasks.
        cus = list()

        for task_count in range(0, 512):
            t = rp.TaskDescription()
            t.executable = '/bin/sleep'
            t.arguments = ['10']
            cus.append(t)

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        tasks = tmgr.submit_tasks(cus)

        # Wait for all tasks to reach a terminal state (DONE or FAILED).
        tmgr.wait_tasks()

        pmgr.cancel_pilots ()

        for task in tasks:
            print("* Task %s state: %s, exit code: %s"
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
        #   session.close (terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


# -------------------------------------------------------------------------------
