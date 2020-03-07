#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
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

    print("[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state))


# ------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):
    """ this callback is invoked on all unit state changes """

    print("[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state))


# ------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):
    """ 
    this callback is called when the size of the unit managers wait_queue
    changes.
    """
    print("[Callback]: UnitManager  '%s' wait_queue_size changed to %s."
        % (umgr.uid, wait_queue_size))

    pilots = umgr.get_pilots ()
    for pilot in pilots:
        print("pilot %s: %s" % (pilot.uid, pilot.state))

    if wait_queue_size == 0:
        for pilot in pilots:
            if pilot.state in [rp.PENDING_LAUNCH,
                                rp.LAUNCHING     ,
                                rp.PENDING_ACTIVE]:
                print("cancel pilot %s" % pilot.uid)
                umgr.remove_pilot (pilot.uid)
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
    session = rp.Session(name=session_name)
    print("session id: %s" % session.uid)

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # prepare some input files for the compute units
        os.system ('hostname > file1.dat')
        os.system ('date     > file2.dat')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager (session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback (pilot_state_cb)

        # Define a 4-core local pilot that runs for 10 minutes and cleans up
        # after itself.
        pdesc1 = rp.ComputePilotDescription()
        pdesc1.resource = "localhost"
        pdesc1.runtime  = 10  # minutes
        pdesc1.cores    =  2

        pdesc2 = rp.ComputePilotDescription()
        pdesc2.resource = "localhost"
        pdesc2.runtime  = 10  # minutes
        pdesc2.cores    =  2

        # Launch the pilots
        pilots = pmgr.submit_pilots ([pdesc1, pdesc2])

        # wait for them to become active
        pmgr.wait_pilots (state=[rp.ACTIVE, rp.DONE, rp.FAILED])


        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager (session   = session,
                               scheduler = rp.SCHEDULER_BACKFILLING)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback (unit_state_cb, rp.UNIT_STATE)

        # Register also a callback which tells us when all units have been
        # assigned to pilots
        umgr.register_callback(wait_queue_size_cb,   rp.WAIT_QUEUE_SIZE)


        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots (pilots)

        # Create a workload of ComputeUnits (tasks). Each compute unit
        # uses /bin/cat to concatenate two input files, file1.dat and
        # file2.dat. The output is written to STDOUT. cu.environment is
        # used to demonstrate how to set environment variables within a
        # ComputeUnit - it's not strictly necessary for this example. As
        # a shell script, the ComputeUnits would look something like this:
        #
        #    export INPUT1=file1.dat
        #    export INPUT2=file2.dat
        #    /bin/cat $INPUT1 $INPUT2
        #
        cuds = []
        for unit_count in range(0, 32):
            cud = rp.ComputeUnitDescription()
            cud.executable    = "/bin/sh"
            cud.environment   = {'INPUT1': 'file1.dat', 'INPUT2': 'file2.dat'}
            cud.arguments     = ["-l", "-c", "cat $INPUT1 $INPUT2; sleep 10"]
            cud.cores         = 1
            cud.input_staging = ['file1.dat', 'file2.dat']
            cud.restartable   = True

            cuds.append(cud)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cuds)

        # the pilots have a total of 4 cores, and run for 10 min.  A CU needs about
        # 10 seconds, so we can handle about 24 units per minute, and need a total
        # of about 3 minutes.  We now wait for 60 seconds, and then cancel the first
        # pilot.  The 2 units currently running on that pilot will fail, all others
        # should get rescheduled to the other pilot.
        time.sleep(60)
        pilots[0].cancel()

        # Wait for all compute units to reach a terminal state (DONE or FAILED).
        umgr.wait_units()

        print('units all completed')
        print('----------------------------------------------------------------------')

        for unit in units:
            unit.wait()

        for unit in units:
            print("* Task %s (executed @ %s) state %s, exit code: %s, \
                  started: %s, finished: %s, stdout: %s" % (unit.uid, 
                  unit.execution_locations, unit.state, unit.exit_code, 
                  unit.start_time, unit.stop_time, unit.stdout))

        # delete the test data files
        os.system('rm file1.dat')
        os.system('rm file2.dat')

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
