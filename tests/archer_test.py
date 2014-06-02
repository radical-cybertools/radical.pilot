import os
import sys
import radical.pilot

# ARCHER:
# =======
# 
# You can use radical.pilot only locally since compute nodes can't dial out at all.
# You also need to install a local MongoDB database:
#
#   wget http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.6.1.tgz
#   tar xzf mongodb-linux-x86_64-2.6.1.tgz
#   mkdir $HOME/mongodata
#   ./mongodb-linux-x86_64-2.6.1/bin/mongod --dbpath=$HOME/mongodata
#
# Create a virtualenv
#
#   wget --no-check-certificate https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.10.tar.gz
#   tar xzf virtualenv-1.10.tar.gz
#   python virtualenv-1.10/virtualenv.py $HOME/RP
#   source $HOME/RP/bin/activate
#
# Download and install RADICAL-Pilot: 
#
#   git clone https://github.com/radical-cybertools/radical.pilot.git
#   cd radical.pilot
#   git checkout feature/archer
#   easy_install .
# 
# Run this script with the local MongoDB server. The hostname in the 
# mongodb URL is the hostname of the login node on which you started
# the server, *not* localhost. 
#
#   cd tests
#  
# In archer_test.py change line `pdesc.sandbox` to appropriate path.
# Next, run the example
#
#   export RADICAL_PILOT_DBURL=mongodb://eslogin007:27017
#   RADICAL_PILOT_VERBOSE=info python archer_test.py
#

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to http://docs.mongodb.org.
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)


# RCONF points to the resource configuration files. Read more about resource 
# configuration files at http://saga-pilot.readthedocs.org/en/latest/machconf.html
RCONF  = ["file://localhost/%s/../configs/local.json" % os.getcwd()]

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

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session, resource_configurations=RCONF)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a 32-core on stamped that runs for 15 mintutes and 
        # uses $HOME/radical.pilot.sandbox as sandbox directoy. 
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource         = "archer.ac.uk"
        pdesc.project          = "e290"  # archer 'project group'
        pdesc.sandbox          = "/home/antons/SINON-WORK/experiments"
        pdesc.runtime          = 10
        pdesc.cores            = 2 
        pdesc.pilot_agent_priv = "radical-pilot-test-agent-archer.py"
        pdesc.agent_worker     = "agent-worker.py"
        pdesc.cleanup          = False


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
        #    /bin/cat $INPUT1  $INPUT2
        #
        compute_units = []

        for unit_count in range(0, 4):

            mpi_test_task = radical.pilot.ComputeUnitDescription()
            mpi_test_task.executable  = "/bin/date"
            mpi_test_task.cores       = 1
            #mpi_test_task.output_data = ["STDOUT"]
            compute_units.append(mpi_test_task)

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

        # Wait for all compute units to reach a terminal state (DONE or FAILED).
        umgr.wait_units()

        for unit in units:
            print "* Task %s - state: %s, exit code: %s, started: %s, finished: %s" \
                % (unit.uid, unit.state, unit.exit_code, unit.start_time, unit.stop_time)

        session.close()
        sys.exit(0)

    except radical.pilot.PilotException, ex:
        # Catch all exceptions and exit with and error.
        print "Error during execution: %s" % ex
        sys.exit(1)

