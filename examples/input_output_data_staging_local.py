import os
import sys
import time
import radical.pilot

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!
#


# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to http://docs.mongodb.org.
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)


#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """
    pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """

    print "[Callback]: ComputePilot '%s' state changed to %s." % (pilot.uid, state)

    if state == radical.pilot.FAILED:
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_change_cb(unit, state):
    """
    unit_state_change_cb() is a callback function. It gets called very
    time a ComputeUnit changes its state.
    """

    print "[Callback]: ComputeUnit '%s' state changed to %s." % (unit.uid, state)

    if state == radical.pilot.FAILED:
        print "            Log: %s" % unit.log[-1]


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # prepare some input files for the compute units
    os.system ('hostname > file1.dat')
    os.system ('date     > file2.dat')

    # Create a new session. A session is the 'root' object for all other
    # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
    # well as security crendetials.
    session = radical.pilot.Session(database_url=DBURL)

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = radical.pilot.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define a 2-core local pilot that runs for 10 minutes and cleans up
    # after itself.
    pdesc = radical.pilot.ComputePilotDescription()
    pdesc.resource = "localhost"
    pdesc.runtime  = 5 # Minutes
    pdesc.cores    = 2
    pdesc.cleanup  = True

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    # Create a workload of 8 ComputeUnits (tasks). Each compute unit
    # uses /bin/cat to concatenate two input files, file1.dat and
    # file2.dat. The output is written to result.dat.
    #
    #    /bin/bash -lc "/bin/cat file1.dat file2.dat > result.dat"
    #
    compute_units = []

    for unit_count in range(0, 16):
        cu = radical.pilot.ComputeUnitDescription()
        cu.executable     = "/bin/bash"
        cu.arguments      = ["-l", "-c", "'cat ./file1.dat ./file2.dat " \
                             " > result-%s.dat'" % unit_count]
        cu.cores          = 1
        cu.input_staging  = ["./file1.dat", "./file2.dat"]
        cu.output_staging = ["result-%s.dat" % unit_count]

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

