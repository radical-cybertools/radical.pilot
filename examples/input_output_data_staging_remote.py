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
def unit_state_change_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


# ------------------------------------------------------------------------------
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

    # Define a 2-core local pilot that runs for 10 minutes and cleans up
    # after itself.
    pdesc = rp.ComputePilotDescription()
    pdesc.resource  = "sierra.futuregrid.org"
    pdesc.runtime   = 15 # minutes
    pdesc.cores     = 8
    # pdesc.project = "TG-MCB090174"
    pdesc.cleanup   = True

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    # Create a workload of 8 ComputeUnits (tasks). Each compute unit
    # uses /bin/cat to concatenate two input files, file1.dat and
    # file2.dat. The output is written to result.dat.
    #
    #    /bin/bash -lc "/bin/cat file1.dat file2.dat > result.dat"
    #
    compute_units = []

    for unit_count in range(0, 32):
        cu = rp.ComputeUnitDescription()
        cu.executable     = "/bin/bash"
        cu.arguments      = ["-l", "-c", "'cat ./file1.dat ./file2.dat " \
                             " > result-%s.dat'" % unit_count]
        cu.cores          = 1
        cu.input_staging  = ["./file1.dat", "./file2.dat"]
        cu.output_staging = ["result-%s.dat" % unit_count]

        compute_units.append(cu)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(
        session=session,
        scheduler=rp.SCHED_DIRECT_SUBMISSION)

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

