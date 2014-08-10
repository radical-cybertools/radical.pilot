import os
import sys
import radical.pilot as rp

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # Create a new session. A session is the 'root' object for all other
    # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
    # well as security crendetials.
    session = rp.Session()

    # Add an ssh identity to the session.
    c = rp.Context('ssh')
    session.add_context(c)

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define a N-core on fs2 that runs for X minutes and
    # uses $HOME/radical.pilot.sandbox as sandbox directory.
    pdesc = rp.ComputePilotDescription()
    pdesc.resource         = "fs2.das4.science.uva.nl"
    pdesc.runtime          = 5 # X minutes
    pdesc.cores            = 16 # N cores
    pdesc.cleanup          = False

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    cud_list = []

    for unit_count in range(0, 10):
        mpi_test_task = rp.ComputeUnitDescription()

        mpi_test_task.pre_exec    = ["source $HOME/.virtualenv/mpi4py/bin/activate", "module load openmpi/gcc"]
        mpi_test_task.executable  = "python"
        mpi_test_task.arguments   = ["$HOME/software/bin/helloworld_mpi.py"]

        mpi_test_task.cores       = 16
        mpi_test_task.mpi         = True

        cud_list.append(mpi_test_task)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(
        session=session,
        scheduler=rp.SCHED_DIRECT_SUBMISSION)

    # Register our callback with the UnitManager. This callback will get
    # called every time any of the units managed by the UnitManager
    # change their state.
    umgr.register_callback(unit_state_change_cb)

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
        print "* Task %s - state: %s, exit code: %s, started: %s, finished: %s, stdout: %s" \
            % (unit.uid, unit.state, unit.exit_code, unit.start_time, unit.stop_time, "n.a.")

    session.close()

