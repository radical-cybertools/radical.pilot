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
#
# Note: because of firewall issues, your MongoDB needs to run on a port within
# the range 20000-25000.
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
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security credentials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add an x509 identity to the session.
        c = radical.pilot.Context('x509')
        c.user_proxy = '/Users/mark/proj/myproxy/xsede.x509'
        c.life_time = 3600
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a X-core on stamped that runs for N minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directoy. 
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource         = "supermuc.lrz.de"
        pdesc.runtime          = 5 # N minutes
        pdesc.cores            = 64 # X cores
        pdesc.cleanup          = False
        #pdesc.project          = "e290"
        #pdesc.sandbox          = "/fs4/e290/e290/marksant/radical.pilot.sandbox"
        #pdesc.working_dir      = "/fs4/e290/e290/$USER/radical.pilot.sandbox"

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        cud_list = []

        for unit_count in range(0, 4):
            mpi_test_task = radical.pilot.ComputeUnitDescription()
            #mpi_test_task.pre_exec    = ["module load python", "source /fs4/e290/e290/marksant/cuve/bin/activate", "module swap PrgEnv-cray PrgEnv-gnu"]
            mpi_test_task.executable  = "python"
            mpi_test_task.arguments   = ["/home/hpc/pr87be/di29sut/software/bin/helloworld_mpi.py"]
            mpi_test_task.cores       = 32
            mpi_test_task.mpi         = True
            cud_list.append(mpi_test_task)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

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

        session.close(delete=False)
        sys.exit(0)

    except radical.pilot.PilotException, ex:
        # Catch all exceptions and exit with and error.
        print "Error during execution: %s" % ex
        sys.exit(1)

