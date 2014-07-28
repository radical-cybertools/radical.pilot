import os
import sys
import radical.pilot

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!
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
        print "            Log: %s" % pilot.log[-1]


#------------------------------------------------------------------------------
#
def unit_state_change_cb(unit, state):
    """unit_state_change_cb() is a callback function. It gets called very
time a ComputeUnit changes its state.
"""
    print "[Callback]: ComputeUnit '{0}' state changed to {1}.".format(
        unit.uid, state)
    if state == radical.pilot.states.FAILED:
        print " Log: %s" % unit.log[-1]

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
        # well as security crendetials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add an ssh identity to the session.
        c = radical.pilot.Context('ssh')
        c.user_id = "tg802352"
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a 32-core on stamped that runs for 15 mintutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directoy.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = "stampede.tacc.utexas.edu"
        pdesc.runtime  = 15 # minutes
        pdesc.cores    = 32
        pdesc.cleanup  = True

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

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

        # Create a ComputeUnit(task) which would transfer the files to
        # the remote host.These files are data that are shared between
        # all of the actual tasks(containing the kernels).Since each of
        # the tasks use the same data, it is better to transfer all the
        # shared data once than with each individual task.

        data_transfer_task =  radical.pilot.ComputeUnitDescription()
        data_transfer_task.executable = "/bin/true"
        data_transfer_task.cores      = 1
        data_transfer_task.input_data = ["./file1.dat","./file2.dat"]

        units=umgr.submit_units(data_transfer_task)

        umgr.wait_units()

        # Get the path to the directory containing the shared data
        shared_input_url = radical.pilot.Url(units.working_directory).path

        # Create a workload of 8 ComputeUnits (tasks). Each compute unit
        # will create a symbolic link of the shared data within its current
        # working directory and then refer to these links just as normal
        # files and use /bin/cat to concatenate two shared data files,
        # file1.dat and file2.dat. The output is written to STDOUT.

        compute_units = []

        for unit_count in range(0, 16):
            cu = radical.pilot.ComputeUnitDescription()
            cu.executable = "/bin/bash"
            cu.arguments  = ["-c",'"ln -s %s/file* . && /bin/cat file1.dat file2.dat > result.dat"' % shared_input_url]
            cu.cores      = 1
            cu.output_data = ["result.dat > result-%s.dat" % unit_count]

            compute_units.append(cu)

        # If the number/type of files as shared data is large/different,
        # a separate file for linking can be invoked which would at the
        # end invoke the kernel(/bin/cat in this example).

        # NOTE: This method is only to use the shared data in read-only purposes.
        # Since symbolic links are used, the actual files from the shared_input_url
        # are opened, any changed made to these files are actually made in the original
        # files. To make changes in the input files in the current directory, a copy of
        # them should be made and not links.

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(compute_units)

        # Wait for all compute units to reach a terminal state (DONE or FAILED).
        umgr.wait_units()

        for unit in units:
            print "* Task %s (executed @ %s) state: %s, exit code: %s, started: %s, finished: %s, output: %s" \
                % (unit.uid, unit.execution_locations, unit.state, unit.exit_code, unit.start_time, unit.stop_time,
                   unit.description.output_data[0].split(">")[1].strip())

        # Close automatically cancels the pilot(s).
        session.close()
        sys.exit(0)

    except radical.pilot.PilotException, ex:
        # Catch all exceptions and exit with and error.
        print "Error during execution: %s" % ex
        sys.exit(1)


