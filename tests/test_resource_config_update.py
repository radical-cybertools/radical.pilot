
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
    # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
    # well as security crendetials.
    session = rp.Session()

    # Add an ssh identity to the session.
    c = rp.Context('ssh')
  # c.user_id = 'merzky'
    session.add_context(c)

    # Get the config entry specific for stampede
    s = session.get_resource_config('xsede.stampede')
    print 'Default queue of stampede is: "%s".' % s['default_queue']

    # Build a new one based on Stampede's
    rc = rp.ResourceConfig(s)
    #rc.name = 'testing'

    # And set the queue to development to get a faster turnaround
    rc.default_queue = 'development'

    # Now add the entry back to the PM
    session.add_resource_config(rc)

    # Get the config entry specific for stampede
    s = session.get_resource_config('xsede.stampede')
    #s = res['testing']
    print 'Default queue of stampede after change is: "%s".' % s['default_queue']
    assert (s['default_queue'] == 'development')

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)


    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define a 32-core on stamped that runs for 15 mintutes and 
    # uses $HOME/radical.pilot.sandbox as sandbox directoy. 
    pdesc = rp.ComputePilotDescription()
    pdesc.resource  = "xsede.stampede"
    pdesc.runtime   = 10 # minutes
    pdesc.cores     = 4
    pdesc.cleanup   = True
    pdesc.project   = 'TG-MCB090174'

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

    compute_units = []
    for unit_count in range(0, 4):
        cu = rp.ComputeUnitDescription()
        cu.executable  = "/bin/date"
        cu.cores       = 1

        compute_units.append(cu)

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    units = umgr.submit_units(compute_units)

    # Wait for all compute units to reach a terminal state (DONE or FAILED).
    umgr.wait_units()

    if not isinstance(units, list):
        units = [units]
    for unit in units:
        print "* Task %s (executed @ %s) state: %s, exit code: %s, started: %s, finished: %s, output: %s" \
            % (unit.uid, unit.execution_locations, unit.state, unit.exit_code, unit.start_time, unit.stop_time,
               unit.stdout)

        assert (unit.state == rp.DONE)

    # Close automatically cancels the pilot(s).
    session.close()


