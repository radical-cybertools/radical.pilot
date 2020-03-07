#!/usr/bin/env python

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

    print("[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state))

    if state == rp.FAILED:
        sys.exit (1)


# ------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):
    """ this callback is invoked on all unit state changes """

    print("[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state))

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
    session = rp.Session(name=session_name)
    print("session id: %s" % session.uid)

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a X-core on stamped that runs for N minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directoy.
        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "xsede.stampede"
        pdesc.runtime  = 15
        pdesc.cores    = 16
        pdesc.project  = "TG-MCB090174"

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        cud_list = []

        for unit_count in range(0, 4):
            cu = rp.ComputeUnitDescription()
            cu.pre_exec      = ["module load python intel mvapich2 mpi4py"]
            cu.executable    = "python"
            cu.arguments     = ["helloworld_mpi.py"]
            cu.input_staging = ["helloworld_mpi.py"]

            # These two parameters are relevant to MPI execution:
            #   'cores' sets the number of cores required by the task
            #   'mpi' identifies the task as an MPI taskg
            cu.cores         = 8
            cu.mpi           = True


            cud_list.append(cu)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager(
            session=session,
            scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cud_list)

        # Wait for all compute units to reach a terminal state (DONE or FAILED).
        umgr.wait_units()

        if not isinstance(units, list):
            units = [units]
        for unit in units:
            print("* Task %s - state: %s, exit code: %s, started: %s, \
                   finished: %s, stdout: %s" % (unit.uid, unit.state,
                                                unit.exit_code, unit.start_time,
                                                unit.stop_time, unit.stdout))

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

