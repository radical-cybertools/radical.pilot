import sys
import sagapilot

# DBURL points to a MongoDB server. For installation of a MongoDB server, please
# refer to the MongoDB website: http://docs.mongodb.org/manual/installation/
DBURL = "mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017"


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    def pilot_state_cb(pilot, state):
        """pilot_state_change_cb is a callback function. It handles ComputePilot
        state changes. Most importantly, it stopps the script if the ComputePilot
        ends up in 'FAILED' state.
        """
        print "[Callback]: ComputePilot '{0}' state changed to {1}.".format(
            pilot.uid, state)

        if state == sagapilot.states.FAILED:
            print "[Callback]: ComputePilot '{0}' failed. Exiting.".format(
                pilot.uid)
            session.destroy()
            sys.exit(1)

        elif state == sagapilot.states.DONE:
            print "[Callback]: ComputePilot '{0}' done. Exiting.".format(
                pilot.uid)
            session.destroy()
            sys.exit(0)

    def unit_state_change_cb(unit, state):
        """unit_state_change_cb is a callback function. It handles ComputeUnit
        state changes.
        """
        print "[Callback]: ComputeUnit '{0}' state changed to {1}.".format(
            unit.uid, state)
        if state == sagapilot.states.FAILED:
            print "            Log: %s" % unit.log[-1]

    try:
        # Create a new session. A session is the 'root' object for all other
        # SAGA-Pilot objects. It encapsualtes the MongoDB connection(s) as
        # well as security crendetials.
        session = sagapilot.Session(database_url=DBURL)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = sagapilot.PilotManager(session=session)

        # Register a callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a 2-core local pilot that runs for 10 minutes.
        pdesc = sagapilot.ComputePilotDescription()
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
            cu = sagapilot.ComputeUnitDescription()
            cu.environment = {"INPUT1": "file1.dat", "INPUT2": "file2.dat"}
            cu.executable = "/bin/cat"
            cu.arguments = ["$INPUT1", "$INPUT2"]
            cu.cores = 1
            cu.input_data = ["./file1.dat", "./file2.dat"]

            compute_units.append(cu)

        cu2 = sagapilot.ComputeUnitDescription()
        cu2.executable = "/bin/date"
        compute_units.append(cu2)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = sagapilot.UnitManager(
            session=session,
            scheduler=sagapilot.SCHED_DIRECT_SUBMISSION)

        # Register a callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_change_cb)

        # Add the previsouly created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(compute_units)

        # # Wait for all compute units to finish.
        umgr.wait_units()

        for unit in umgr.get_units():
            # Print some information about the unit.
            print "{0}".format(str(unit))

            # Get the stdout and stderr streams of the ComputeUnit.
            print "  STDOUT: {0}".format(unit.stdout)
            print "  STDERR: {0}".format(unit.stderr)

        # Print some information about the pilot before we cancel it.
        print "{0}".format(str(pilot))

        # Cancel all pilots.
        pmgr.cancel_pilots()

        # Remove session from database
        session.destroy()

    except sagapilot.SagapilotException, ex:
        print "Error: %s" % ex
