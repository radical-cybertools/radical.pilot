#!/usr/bin/env python3

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os

os.environ['RADICAL_PILOT_BULK_CB'] = 'True'

import random

import radical.pilot as rp

# READ: The RADICAL-Pilot documentation:
#   https://radicalpilot.readthedocs.io/en/stable/
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if
# you want to see what happens behind the scenes!


# ------------------------------------------------------------------------------
#
def pilot_state_cb(pilots):
    """
    this callback is invoked on all pilot state changes
    """

    # Callbacks happen in a different thread than the main application thread,
    # i.e., they are asynchronous.  However, this means that a 'sys.exit()'
    # does not end the application but just a thread (in this case the
    # pilot_manager_controller thread).  Thus, we wrap all threads in their own
    # try/except clauses, and then translate the `sys.exit()` into a
    # 'thread.interrupt_main()' call. This raises a 'KeyboardInterrupt' in the
    # main thread which can be interpreted by your application, for example to
    # initiate a clean shutdown via `session.close()` (see code below). The
    # same `KeyboardShutdown` is also raised when you interrupt the application
    # via `^C`.
    #
    # Note that other error handling semantics are available, depending on your
    # application requirements. For example, upon a pilot failure, the
    # application could spawn a replacement pilot, or reduce the number of
    # tasks to match the remaining set of pilots.

    for pilot in pilots:
        print("[Callback]: Pilot '%s' state: %s." % (pilot.uid, pilot.state))

        if pilot.state == rp.FAILED:
            print(pilot.log[-1])  # Get the last log message


# -----------------------------------------------------------------------------
#
def task_state_cb(tasks):
    """ this callback is invoked on all task state changes """

    # The approach to task state callbacks is the same as the one to
    # pilot state callbacks. Only difference is that task state
    # callbacks are invoked by the task manager on changes of task
    # states.
    #
    # The example below does not really create any Task object, we only
    # include the callback here for documentation on the approaches to error
    # handling.
    #
    # Note that other error handling semantics are available, depending on your
    # application requirements. For example, upon tasks failure, the
    # application could spawn replacement tasks, or spawn a pilot on a
    # different resource which might be better equipped to handle the task
    # payload.

    for task in tasks:
        print('  task %s: %s' % (task.uid, task.state))

        if task.state == rp.FAILED:
            print('                  : %s' % task.stderr)


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # This example shows how simple error handling can be asynchronously
    # implemented using callbacks (see above).

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyway...
    session = rp.Session()
    print("session id: %s" % session.uid)

    # all other RP code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus
    # tear the whole RP stack down via a 'session.close()' call in the
    # 'finally' clause...
    try:

        # create the task and pilot managers.
        tmgr = rp.TaskManager(session=session)
        pmgr = rp.PilotManager(session=session)

        # Register our callbacks with the managers. The callbacks will get
        # called every time any of the pilots or tasks change their state,
        # including on FAILED states.
        tmgr.register_callback(task_state_cb)
        pmgr.register_callback(pilot_state_cb)

        # Create a local pilot.
        pd = rp.PilotDescription()
        pd.resource  = "local.localhost"
        pd.cores     = 64
        pd.runtime   = 60

        pilot = pmgr.submit_pilots(pd)
        tmgr.add_pilots(pilot)


        # we submit n tasks, some of which will fail.
        n    = 3 * 1024
        tds = list()
        for _ in range(n):
            td = rp.TaskDescription()
            if random.random() < 0.5:
                td.executable = '/bin/true'
            else:
                td.executable = '/bin/fail'
            tds.append(td)

        # submit the tasks...
        cus = tmgr.submit_tasks(tds)

        # ... and wait for their completion.
        state = tmgr.wait_tasks(state=rp.FINAL)


    except Exception as e:
        # Something unexpected happened in the pilot code above.
        print("caught Exception: %s" % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also
        # catch SystemExit, which gets raised if the main thread exits for
        # some other reason.
        print("need to exit now: %s" % e)

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print("closing session")
        session.close()


# ------------------------------------------------------------------------------

