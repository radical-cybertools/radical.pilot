import os
import sys
import radical.pilot as rp

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
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

        # Define a pilot on stampede of in total 32 cores that spans two nodes,
        # runs for 15 mintutes and uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = rp.ComputePilotDescription()
        pdesc.resource  = "stampede.tacc.utexas.edu"
        pdesc.runtime   = 15 # minutes
        pdesc.cores     = 32 
        pdesc.cleanup   = False

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Number of cores for respective task
        #task_cores = [1 for _ in range(32)]
        task_cores = [3 for _ in range(32)]
        #task_cores = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]

        # Seconds of sleep for respective task
        task_sleep = [30 for _ in range(32)]

        compute_unit_descriptions = []
        for unit_no in range(32):
            cud = rp.ComputeUnitDescription()
            #cud.executable  = ""
            cud.arguments   = ["/bin/sleep %d && /bin/date > unit-%s.dat" % (task_sleep[unit_no], unit_no)]
            cud.cores       = task_cores[unit_no]

            compute_unit_descriptions.append(cud)

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
        units = umgr.submit_units(compute_unit_descriptions)

        # Wait for all compute units to reach a terminal state (DONE or FAILED).
        umgr.wait_units()

        for unit in units:
            print "* Task %s (executed @ %s) state: %s, exit code: %s, started: %s, finished: %s, output: %s" \
                % (unit.uid, unit.execution_locations, unit.state, unit.exit_code, unit.start_time, unit.stop_time,
                   unit.stdout)

        session.close(delete=False)
        sys.exit(0)

    except rp.PilotException, ex:
        # Catch all exceptions and exit with and error.
        print "Error during execution: %s" % ex
        sys.exit(1)
