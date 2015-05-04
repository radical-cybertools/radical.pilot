#!/usr/bin/env python


import sys
import radical.pilot as rp


# ##############################################################################
# #87: error on adding callback to ComputeUnit
# ##############################################################################

cb_counter = 0

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

    global cb_counter
    cb_counter += 1

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
    # pmgr.register_callback(pilot_state_cb)

    # Define a 2-core local pilot that runs for 10 minutes.
    pdesc = rp.ComputePilotDescription()
    pdesc.resource = "local.localhost"
    pdesc.runtime  = 10
    pdesc.cores    = 1

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)
    pilot.register_callback(pilot_state_cb)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager (session=session,
                           scheduler=rp.SCHED_DIRECT_SUBMISSION)

    # Add the previsouly created ComputePilot to the UnitManager.
    umgr.add_pilots(pilot)

    # Create a workload of 8 ComputeUnits (tasks).
    unit_descr = rp.ComputeUnitDescription()
    unit_descr.executable = "/bin/sleep"
    unit_descr.arguments  = ['10']
    unit_descr.cores = 1

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    units = umgr.submit_units(unit_descr)

    # Wait for all compute units to finish.
    for unit in umgr.get_units():
        unit.register_callback(unit_state_change_cb)

    umgr.wait_units()

    global cb_counter
    assert (cb_counter > 1) # one invokation to capture final state

    # Cancel all pilots.
    pmgr.cancel_pilots()
    pmgr.wait_pilots()

except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    session.close()


