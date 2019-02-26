#!/usr/bin/env python


import sys
import radical.pilot as rp

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if  state in [rp.FAILED, rp.DONE] :
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_change_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

    if  state == rp.FAILED :
        print "                         '%s' stderr: %s." % (unit.uid, unit.stderr)
        print "                         '%s' stdout: %s." % (unit.uid, unit.stdout)
        sys.exit (1)


#------------------------------------------------------------------------------
#
# Create a new session. A session is the 'root' object for all other
# RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
# well as security contexts.
session = rp.Session()

try:

    # Add an ssh identity to the session.
    c = rp.Context('ssh')
    session.add_context(c)

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define a X-core pilot that runs for N minutes.
    # Trestles has 32 cores per node.
    pdesc = rp.ComputePilotDescription()
    pdesc.resource = "xsede.trestles"
    pdesc.runtime  = 20  # N minutes
    pdesc.cores    = 683 # X cores

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    cud_list = []
    for unit_count in range(0, 256):

        cud = rp.ComputeUnitDescription()

        cud.executable = "/bin/sleep"
        cud.arguments = ["300"]
        cud.mpi = False
        cud.cores = 8

        cud_list.append(cud)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(session=session, scheduler=rp.SCHEDULER_BACKFILLING)

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
        
        assert (unit.state == rp.DONE)

except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    session.close()

