
import sys
import radical.pilot as rp

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


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # Create a new session. A session is the 'root' object for all other
    # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
    # well as security contexts.
    session = rp.Session()

    # Add an ssh identity to the session.
    cred = rp.Context('ssh')
    session.add_context(cred)

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define a X-core on stamped that runs for N minutes and
    # uses $HOME/radical.pilot.sandbox as sandbox directory.
    pdesc = rp.ComputePilotDescription()
    pdesc.resource         = "ucar.yellowstone"
    pdesc.runtime          = 15 # N minutes
    pdesc.cores            = 32 # X cores
    pdesc.cleanup          = False
    pdesc.project          = "URTG0003"

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    cud_list = []

    for unit_count in range(0, 64):
        test_task = rp.ComputeUnitDescription()
        test_task.executable  = "/bin/bash"
        test_task.arguments   = ["-l", "-c", "'hostname -f && uname -a && sleep 30'"]
        cud_list.append(test_task)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(
        session=session,
        scheduler=rp.SCHED_DIRECT_SUBMISSION)

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

    session.close()

