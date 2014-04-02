import os
import sys
import radical.pilot
import saga

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICALPILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICALPILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

# RCONF points to the resource configuration files. Read more about resource
# configuration files at http://saga-pilot.readthedocs.org/en/latest/machconf.html
RCONF = ["https://raw.github.com/radical-cybertools/radical.pilot/master/configs/xsede.json",
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
        cred = radical.pilot.SSHCredential()
        cred.user_id = "oweidner"
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
        pdesc.resource = "sierra.futuregrid.org"
        pdesc.runtime = 15 # minutes
        pdesc.cores = 32

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
        data_transfer_task.cores = 1
        data_transfer_task.input_data = ["./file1.dat","./file2.dat"]

        units=umgr.submit_units(data_transfer_task)

        umgr.wait_units()

        # Get the path to the directory containing the shared data
        shared_input_url = saga.Url(units.working_directory).path

        # Create a workload of 8 ComputeUnits (tasks). Each compute unit
        # will create a symbolic link of the shared data within its current
        # working directory and then refer to these links just as normal
        # files and use /bin/cat to concatenate two shared data files,
        # file1.dat and file2.dat. The output is written to STDOUT.

        compute_units = []

        for unit_count in range(0, 8):
            cu = radical.pilot.ComputeUnitDescription()
            cu.executable = "/bin/bash"
            cu.arguments = ["-c",'"ln -s %s/file* . && /bin/cat file1.dat file2.dat > file3.dat"'%shared_input_url]
            cu.cores = 1

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

        # Wait for all compute units to finish.
        umgr.wait_units()

        for unit in umgr.get_units():
            # Print some information about the unit.
            #print "\n{0}".format(str(unit))

            # Get the stdout and stderr streams of the ComputeUnit.
            print " STDOUT: {0}".format(unit.stdout)
            print " STDERR: {0}".format(unit.stderr)

        session.close()

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

