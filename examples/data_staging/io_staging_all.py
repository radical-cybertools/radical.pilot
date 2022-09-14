#!/usr/bin/env python3

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys

import radical.utils as ru
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
        pdesc.cores    = 8
        pdesc.runtime  = 5  # Minutes
      # pdesc.cleanup  = True

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        import time
        time.sleep(10)

        check = pilot.stage_in([
            {'source': '/etc/passwd',
             'target': 'pilot:///pilot_stage_in_to_pilot.tmp',
             'action': rp.TRANSFER},
            {'source': '/etc/passwd',
             'target': 'session:///pilot_stage_in_to_session.tmp',
             'action': rp.TRANSFER},
            {'source': '/etc/passwd',
             'target': 'resource:///pilot_stage_in_to_resource.tmp',
             'action': rp.TRANSFER},
            {'source': '/etc/passwd',
             'target': 'endpoint:///tmp/pilot_stage_in_to_endpoint.tmp',
             'action': rp.TRANSFER}])

        check += pilot.stage_out([
            {'source': 'pilot:///bootstrap_0.sh',
             'target': 'client:///pilot_stage_out_to_client.tmp',
             'action': rp.TRANSFER},
            {'source': 'pilot:///bootstrap_0.sh',
             'target': 'file:///tmp/pilot_stage_out_to_file.tmp',
             'action': rp.TRANSFER},
            {'source': 'pilot:///bootstrap_0.sh',
             'target': 'pwd:///pilot_stage_out_to_pwd.tmp',
             'action': rp.TRANSFER},

            {'source': 'endpoint:///tmp/pilot_stage_in_to_endpoint.tmp',
             'target': 'pwd:///pilot_stage_out_from_endpoint.tmp',
             'action': rp.TRANSFER},
            {'source': 'resource:///pilot_stage_in_to_resource.tmp',
             'target': 'pwd:///pilot_stage_out_from_resource.tmp',
             'action': rp.TRANSFER},
            {'source': 'session:///pilot_stage_in_to_session.tmp',
             'target': 'pwd:///pilot_stage_out_from_session.tmp',
             'action': rp.TRANSFER},
            {'source': 'pilot:///pilot_stage_in_to_pilot.tmp',
             'target': 'pwd:///pilot_stage_out_from_pilot.tmp',
             'action': rp.TRANSFER}])

        for c in check:
            u = ru.Url(c)
            os.system('ls -l %s' % u.path)

        input_sd = [
            {'source': '/etc/passwd',
             'target': 'task:///task_stage_in_to_task.tmp',
             'action': rp.TRANSFER},
            {'source': '/etc/passwd',
             'target': 'pilot:///task_stage_in_to_pilot.tmp',
             'action': rp.TRANSFER},
            {'source': '/etc/passwd',
             'target': 'session:///task_stage_in_to_session.tmp',
             'action': rp.TRANSFER},
            {'source': '/etc/passwd',
             'target': 'resource:///task_stage_in_to_resource.tmp',
             'action': rp.TRANSFER},
            {'source': '/etc/passwd',
             'target': 'endpoint:///tmp/task_stage_in_to_endpoint.tmp',
             'action': rp.TRANSFER}]

        output_sd = [
            {'source': 'task:///result.dat',
             'target': 'client:///task_stage_out_to_client.tmp',
             'action': rp.TRANSFER},
            {'source': 'pilot:///bootstrap_0.sh',
             'target': 'client:///task_stage_out_to_client.tmp',
             'action': rp.TRANSFER},
            {'source': 'pilot:///bootstrap_0.sh',
             'target': 'file:///tmp/task_stage_out_to_file.tmp',
             'action': rp.TRANSFER},
            {'source': 'pilot:///bootstrap_0.sh',
             'target': 'pwd:///task_stage_out_to_pwd.tmp',
             'action': rp.TRANSFER},

            {'source': 'endpoint:///tmp/task_stage_in_to_endpoint.tmp',
             'target': 'pwd:///task_stage_out_from_endpoint.tmp',
             'action': rp.TRANSFER},
            {'source': 'resource:///task_stage_in_to_resource.tmp',
             'target': 'pwd:///task_stage_out_from_resource.tmp',
             'action': rp.TRANSFER},
            {'source': 'session:///task_stage_in_to_session.tmp',
             'target': 'pwd:///task_stage_out_from_session.tmp',
             'action': rp.TRANSFER},
            {'source': 'pilot:///task_stage_in_to_pilot.tmp',
             'target': 'pwd:///task_stage_out_from_pilot.tmp',
             'action': rp.TRANSFER},
            {'source': 'task:///task_stage_in_to_task.tmp',
             'target': 'pwd:///task_stage_out_from_task.tmp',
             'action': rp.TRANSFER}]

        # Create a Task that sorts the local password file and writes the
        # output to result.dat.
        #
        #  The exact command that is executed by the agent is:
        #    "/usr/bin/sort -o result.dat input.dat"
        #
        td = rp.TaskDescription()
        td.executable     = "sort"
        td.arguments      = ["-o", "result.dat", "task_stage_in_to_task.tmp"]
        td.input_staging  = input_sd
        td.output_staging = output_sd

        # Combine the Pilot, the Tasks and a scheduler via
        # a TaskManager object.
        tmgr = rp.TaskManager(session)

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

        print("* Task %s state: %s, exit code: %s" %
              (task.uid, task.state, task.exit_code))

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


# ------------------------------------------------------------------------------

