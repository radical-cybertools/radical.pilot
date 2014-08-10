import os
import sys
import radical.pilot as rp

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # Create a new session. A session is the 'root' object for all other
    # SAGA-Pilot objects. It encapsualtes the MongoDB connection(s) as
    # well as security crendetials.
    session = rp.Session()

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define a 2-core local pilot that runs for 10 minutes.
    pdesc = rp.ComputePilotDescription()
    pdesc.resource = "localhost"
    pdesc.runtime = 10
    pdesc.cores = 2

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    # Create a workload of 8 ComputeUnits (tasks). Each compute unit
    # uses /bin/cat to concatenate two input files, file1.dat and
    # file2.dat. The output is written to STDOUT. cu.environment is
    # used to demonstrate how to set environment variables withih a
    # ComputeUnit - it's not strictly necessary for this example. As
    # a shell script, the ComputeUnits would look something like this:
    #
    #    export INPUT1=file1.dat
    #    export INPUT2=file2.dat
    #    /bin/cat $INPUT1 $INPUT2
    #
    compute_units = []

    for unit_count in range(0, 8):
        cu = rp.ComputeUnitDescription()
        cu.executable = "/bin/date"
        cu.cores = 1
        cu.input_data = ["skeleton.py"]
        cu.output_data = ["STDOUT"]

        compute_units.append(cu)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(
        session=session,
        scheduler=rp.SCHED_DIRECT_SUBMISSION,
        output_transfer_workers=4,
        input_transfer_workers=4)

    # Register our callback with the UnitManager. This callback will get
    # called every time any of the units managed by the UnitManager
    # change their state.
    umgr.register_callback(unit_state_change_cb)

    # Add the previsouly created ComputePilot to the UnitManager.
    umgr.add_pilots(pilot)

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    units = umgr.submit_units(compute_units)

    # Wait for all compute units to finish.
    umgr.wait_units()

    pmgr.cancel_pilots()

    import time
    time.sleep(2)

    print "\n== UNIT STATE HISTORY==\n"

    for unit in umgr.get_units():
        # Print some information about the unit.
        for state in unit.state_history:
            print " * %s: %s\n" % (state.timestamp, state.state)

    print "\n== PILOT STATE HISTORY==\n"

    for state in pilot.state_history:
        print " * %s: %s\n" % (state.timestamp, state.state)

    # Remove session from database
    session.close()

