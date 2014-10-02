
import sys
import radical.pilot as rp

# user ID on archer
USER_ID = 'merzky'


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
    # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
    # well as security crendetials.
    session = rp.Session()

    # Add an ssh identity to the session.
    c = rp.Context('ssh')
    c.user_id = USER_ID
    session.add_context(c)

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    pdesc = rp.ComputePilotDescription()
    pdesc.resource = "epsrc.archer"
    pdesc.project  = "e290"  # archer 'project group'
    pdesc.runtime  = 10
    pdesc.cores    = 56      # there are 24 cores per node on Archer, so this allocates 3 nodes
    pdesc.sandbox  = "/work/e290/e290/%s/radical.pilot.sandbox/" % USER_ID
    pdesc.cleanup  = False

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
    umgr.register_callback(unit_state_change_cb)

    # Add the previsouly created ComputePilot to the UnitManager.
    umgr.add_pilots(pilot)

    # create compute unit descriptions
    cu_descriptions = []
    for unit_count in range(0, 8):

        mpi_test_task = rp.ComputeUnitDescription()
        mpi_test_task.executable  = "/bin/hostname"
        mpi_test_task.cores       = 4
        cu_descriptions.append(mpi_test_task)

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    units = umgr.submit_units(cu_descriptions)

    # Wait for all compute units to reach a terminal state (DONE or FAILED).
    umgr.wait_units()

    for unit in units:
        print "* Task %s - state: %s, exit code: %s, started: %s, finished: %s" \
            % (unit.uid, unit.state, unit.exit_code, unit.start_time, unit.stop_time)

    session.close()

# ------------------------------------------------------------------------------

