import os
import sys
import radical.pilot

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICALPILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICALPILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

# RCONF points to the resource configuration files. Read more about resource 
# configuration files at http://saga-pilot.readthedocs.org/en/latest/machconf.html
RCONF  = ["https://raw.github.com/radical-cybertools/radical.pilot/master/configs/xsede.json",
          "https://raw.github.com/radical-cybertools/radical.pilot/master/configs/futuregrid.json"]

#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """
    print "[Callback]: ComputePilot '{0}' state changed to {1}.".format(
        pilot.uid, state)

    if state == radical.pilot.states.FAILED:
        sys.exit(1)

#------------------------------------------------------------------------------
#
def unit_state_change_cb(unit, state):
    """unit_state_change_cb() is a callback function. It gets called very
    time a ComputeUnit changes its state.
    """
    print "[Callback]: ComputeUnit '{0}' state changed to {1}.".format(
        unit.uid, state)
    if state == radical.pilot.states.FAILED:
        print "            Log: %s" % unit.log[-1]

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
        # well as security crendetials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add an ssh identity to the session.
        cred = radical.pilot.SSHCredential()
        cred.user_id = "tg802352"
        session.add_credential(cred)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session, resource_configurations=RCONF)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a 32-core on stamped that runs for 15 mintutes and 
        # uses $HOME/radical.pilot.sandbox as sandbox directoy. 
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource         = "stampede.tacc.utexas.edu"
        pdesc.runtime          = 15 # minutes
        pdesc.cores            = 32 
        pdesc.pilot_agent_priv = "radical-pilot-agent-mpi.py"

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
            cu = radical.pilot.ComputeUnitDescription()
            cu.environment = {"INPUT1": "file1.dat", "INPUT2": "file2.dat"}
            cu.executable = "/bin/cat"
            cu.arguments = ["$INPUT1", "$INPUT2"]
            cu.cores = 1
            cu.input_data = ["./file1.dat", "./file2.dat"]

            compute_units.append(cu)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

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

        for unit in umgr.get_units():
            # Print some information about the unit.
            print "\n{0}".format(str(unit))

            # Get the stdout and stderr streams of the ComputeUnit.
            print "  STDOUT: {0}".format(unit.stdout)
            print "  STDERR: {0}".format(unit.stderr)

        session.close()

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex
