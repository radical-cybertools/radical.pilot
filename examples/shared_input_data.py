import os
import sys
import radical.pilot as rp

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!


#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # prepare some input files for the compute units
    os.system ('hostname > file1.dat')
    os.system ('date     > file2.dat')

    # Create a new session. A session is the 'root' object for all other
    # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
    # well as security crendetials.
    session = rp.Session()

    # Add an ssh identity to the session.
    c = rp.Context('ssh')
    c.user_id = "merzky"
    session.add_context(c)

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define a pilot that runs for 15 minutes 
    pdesc = rp.ComputePilotDescription()
    pdesc.resource = "india.futuregrid.org"
    pdesc.runtime  = 15 # minutes
    pdesc.cores    = 8
    pdesc.cleanup  = True

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(
        session=session,
        scheduler=rp.SCHED_DIRECT_SUBMISSION)

    # Register our callback with the UnitManager. This callback will get
    # called every time any of the units managed by the UnitManager
    # change their state.
    umgr.register_callback(unit_state_cb)

    # Add the previsouly created ComputePilot to the UnitManager.
    umgr.add_pilots(pilot)

    # Create a ComputeUnit(task) which would transfer the files to
    # the remote host.These files are data that are shared between
    # all of the actual tasks(containing the kernels).Since each of
    # the tasks use the same data, it is better to transfer all the
    # shared data once than with each individual task.

    staging_unit_descr =  rp.ComputeUnitDescription()
    staging_unit_descr.executable    = "/bin/true"
    staging_unit_descr.cores         = 1
    staging_unit_descr.input_staging = ["file1.dat", "file2.dat"]

    staging_unit = umgr.submit_units(staging_unit_descr)

    umgr.wait_units()

    # Get the path to the directory containing the shared data
    shared_input_url = rp.Url(staging_unit.working_directory).path

    # Create a workload of 8 ComputeUnits (tasks). Each compute unit
    # will create a symbolic link of the shared data within its current
    # working directory and then refer to these links just as normal
    # files and use /bin/cat to concatenate two shared data files,
    # file1.dat and file2.dat. The output is written to STDOUT.

    compute_units = []

    for unit_count in range(0, 16):
        cu = rp.ComputeUnitDescription()
        cu.executable     = "/bin/bash"
        cu.arguments      = ["-c",'"ln -s %s/file* . && /bin/cat file1.dat " \
                             "file2.dat > result-%s.dat"' % (shared_input_url, unit_count)]
        cu.cores          = 1
        cu.output_staging = ["result-%s.dat" % unit_count]

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
               unit.description.output_staging[0])

    # Close automatically cancels the pilot(s).
    session.close()

    # delete the test data files
    os.system ('rm file1.dat')
    os.system ('rm file2.dat')
    os.system ('rm result-*.dat')

# ------------------------------------------------------------------------------

