import os
import sys
import radical.pilot

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scenes!
#
# RADICAL-Pilot uses ssh to communicate with the remote resource. The 
# easiest way to make this work seamlessly is to set up ssh key-based
# authentication and add the key to your keychain so you won't be 
# prompted for a password. The following article explains how to set 
# this up on Linux:
#   http://www.cyberciti.biz/faq/ssh-password-less-login-with-dsa-publickey-authentication/

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to http://docs.mongodb.org.
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

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
        retval = 0

        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security contexts.
        session = radical.pilot.Session(database_url=DBURL)

        # Add an ssh identity to the session.
        c = radical.pilot.Context('ssh')
        #c.user_id = 'merzky'
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a N-core on fs2 that runs for X minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource         = "india.futuregrid.org"
        pdesc.runtime          = 5 # X minutes
        pdesc.cores            = 16 # N cores
        pdesc.cleanup          = False

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        cud_list = []

        # Configure the staging directive for input file.
        sd_exec = radical.pilot.StagingDirectives()
        sd_exec.source = 'helloworld_mpi.py'
        sd_exec.target = 'helloworld_mpi.py'
        sd_exec.action = radical.pilot.TRANSFER

        for unit_count in range(0, 4):
            mpi_test_task = radical.pilot.ComputeUnitDescription()

            # NOTE: Default module versions are different on worker nodes and head node,
            #       so test pre_exec's on a worker node and not on the headnode!
            mpi_test_task.pre_exec = ["module load openmpi/1.4.3-gnu python",
                                      "(test -d $HOME/mpive || rm -rf $HOME/mpive && virtualenv $HOME/mpive)",
                                      "source $HOME/mpive/bin/activate",
                                      "(pip freeze | grep -q mpi4py || pip install mpi4py)"
            ]
            mpi_test_task.input_staging  = [sd_exec]
            mpi_test_task.executable  = "python"
            mpi_test_task.arguments   = ["helloworld_mpi.py"]
            mpi_test_task.mpi         = True

            mpi_test_task.cores       = 4

            cud_list.append(mpi_test_task)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            #scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)
            scheduler=radical.pilot.SCHED_LATE_BINDING)

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
                % (unit.uid, unit.state, unit.exit_code, unit.start_time, unit.stop_time, unit.stdout)
            if  unit.state == radical.pilot.FAILED :
                print "STDERR: %s" % unit.stderr
                print "STDOUT: %s" % unit.stdout
                retval = 1

        session.close(delete=False)
        sys.exit(retval)

    except radical.pilot.PilotException, ex:
        # Catch all exceptions and exit with and error.
        print "Error during execution: %s" % ex
        sys.exit(1)
