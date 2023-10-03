#!/usr/bin/env python3

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import time
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


# ------------------------------------------------------------------------------
#
def task_state_cb (task, state):
    """ this callback is invoked on all task state changes """

    print("[Callback]: Task  '%s' state: %s." % (task.uid, state))


# ------------------------------------------------------------------------------
#
def wait_queue_size_cb(tmgr, wait_queue_size):
    """
    this callback is called when the size of the task managers wait_queue
    changes.
    """
    print("[Callback]: TaskManager  '%s' wait_queue_size changed to %s."
        % (tmgr.uid, wait_queue_size))

    pilots = tmgr.get_pilots ()
    for pilot in pilots:
        print("pilot %s: %s" % (pilot.uid, pilot.state))

    if wait_queue_size == 0:
        for pilot in pilots:
            if pilot.state in [rp.PENDING_LAUNCH,
                                rp.LAUNCHING     ,
                                rp.PENDING_ACTIVE]:
                print("cancel pilot %s" % pilot.uid)
                tmgr.remove_pilot (pilot.uid)
                pilot.cancel ()


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
        pmgr = rp.PilotManager (session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback (pilot_state_cb)

        # Define a 4-core local pilot that runs for 10 minutes and cleans up
        # after itself.

        pdesc1 = rp.PilotDescription()
        pdesc1.resource = "local.localhost"
        pdesc1.runtime  = 10  # minutes
        pdesc1.cores    =  2

        pdesc2 = rp.PilotDescription()
        pdesc2.resource = "local.localhost"
        pdesc2.runtime  = 10  # minutes
        pdesc2.cores    =  2

        # Launch the pilots
        pilots = pmgr.submit_pilots([pdesc1, pdesc2])

        # wait for them to become active
        pmgr.wait_pilots (state=[rp.PMGR_ACTIVE, rp.DONE, rp.FAILED])


        # Combine the Pilot, the Tasks and a scheduler via
        # a TaskManager object.
        tmgr = rp.TaskManager (session   = session,
                               scheduler = rp.SCHEDULER_BACKFILLING)

        # Register our callback with the TaskManager. This callback will get
        # called every time any of the tasks managed by the TaskManager
        # change their state.
        tmgr.register_callback (task_state_cb, rp.TASK_STATE)

        # Register also a callback which tells us when all tasks have been
        # assigned to pilots
        tmgr.register_callback(wait_queue_size_cb, rp.WAIT_QUEUE_SIZE)


        # Add the previously created Pilot to the TaskManager.
        tmgr.add_pilots (pilots)

        # Create a workload of restartable Tasks (tasks).
        tds = []
        for task_count in range(0, 32):
            td = rp.TaskDescription()
            td.executable    = "/bin/sleep"
            td.arguments     = ["10"]
            td.restartable   = True

            tds.append(td)

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        tasks = tmgr.submit_tasks(tds)

        # the pilots have a total of 4 cores, and run for 10 min.  A Task needs about
        # 10 seconds, so we can handle about 24 tasks per minute, and need a total
        # of about 3 minutes.  We now wait for 60 seconds, and then cancel the first
        # pilot.  The 2 tasks currently running on that pilot will fail, and
        # maybe 2 more which are being pre-fetched into the pilot at that stage
        # - all others should get rescheduled to the other pilot.
        time.sleep(60)
        pilots[0].wait(state=rp.PMGR_ACTIVE)
        pilots[0].cancel()

        # Wait for all tasks to reach a terminal state (DONE or FAILED).
        tmgr.wait_tasks()

        print('tasks all completed')
        print('----------------------------------------------------------------------')

        for task in tasks:
            task.wait()

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
