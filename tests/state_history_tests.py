#!/usr/bin/env python


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
# Create a new session. A session is the 'root' object for all other
# RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
# well as security crendetials.
session = rp.Session()

try:

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define a 2-core local pilot that runs for 10 minutes.
    pdesc = rp.ComputePilotDescription()
    pdesc.resource = "local.localhost"
    pdesc.runtime  = 10
    pdesc.cores    = 1

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(
        session=session,
        scheduler=rp.SCHEDULER_DIRECT_SUBMISSION,
        output_transfer_workers=4,
        input_transfer_workers=4)

    # Register our callback with the UnitManager. This callback will get
    # called every time any of the units managed by the UnitManager
    # change their state.
    umgr.register_callback(unit_state_change_cb)

    # Add the previsouly created ComputePilot to the UnitManager.
    umgr.add_pilots(pilot)

    # Create a workload of 8 ComputeUnits (tasks).
    compute_units = []
    for unit_count in range(0, 8):
        cu = rp.ComputeUnitDescription()
        cu.executable = "/bin/date"

        compute_units.append(cu)

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    units = umgr.submit_units(compute_units)

    # Wait for all compute units to finish.
    umgr.wait_units()

    pmgr.cancel_pilots()
    pmgr.wait_pilots (state=[rp.CANCELED, rp.DONE, rp.FAILED])


    print "\n== UNIT STATE HISTORY==\n"

    for unit in umgr.get_units():
        # Print some information about the unit.
        print "unit %s" % unit.uid
        states = list()
        for entry in unit.state_history:
            print " * %s: %s" % (entry.timestamp, entry.state)
            states.append (entry.state)
        assert (states)
     #  assert (rp.NEW        in states)
        assert (rp.SCHEDULING in states)
        assert (rp.ALLOCATING in states)
        assert (rp.EXECUTING  in states)
        assert (rp.DONE       in states)

    print "\n== PILOT STATE HISTORY==\n"

    print "pilot %s" % pilot.uid
    states = list()
    for entry in pilot.state_history:
        print " * %s: %s" % (entry.timestamp, entry.state)
        states.append (entry.state)
    assert (states)
    assert (rp.PENDING_LAUNCH in states)
    assert (rp.LAUNCHING      in states)
    assert (rp.PENDING_ACTIVE in states)
    assert (rp.ACTIVE         in states)
    assert (rp.CANCELED       in states)


except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    session.close()

