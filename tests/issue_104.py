#!/usr/bin/env python


import sys
import radical.pilot as rp

# ##############################################################################
# #104: Some ExecWorker(s) out of memory with a bag of 4096 tasks
# ##############################################################################

# ATTENTION:
#
# This test consumes about 20   CPU hours on stampede -- it is thus not run as
# part of the usual issue testing.

# NOTE:
#
# as is, this script will not be able to reproduce issue #104


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
    pdesc.resource = "xsede.stampede"
    pdesc.project  = "TG-MCB090174"
    pdesc.runtime  = 30
    pdesc.cores    = 4096

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    # Create a workload of 8 ComputeUnits (tasks). Each compute unit
    # uses /bin/cat to concatenate two input files, file1.dat and
    # file2.dat. The output is written to STDOUT. cu.environment is
    # used to demonstrate how to set environment variables withih a
    # ComputeUnit - it's not strictly necessary for this example. As
    # a shell script, the ComputeUnits would look something like this:
    #
    #    export INPUT1=file1.dat
    #    export INPUT2=file2.dat
    #    /bin/cat $INPUT1 $INPUT2
    #
    compute_units = []

    for unit_count in range(0, 4 * 4096):
        cu = rp.ComputeUnitDescription()
        cu.executable = "/bin/sleep"
        cu.arguments = ["60"]
        cu.cores = 1

        compute_units.append(cu)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(
        session=session,
        scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

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

    # Wait for all compute units to finish.
    umgr.wait_units()

    for unit in umgr.get_units():
        # Print some information about the unit.
        print "%s" % str(unit)

        # Get the stdout and stderr streams of the ComputeUnit.
        print "  STDOUT: %s" % unit.stdout
        print "  STDERR: %s" % unit.stderr

    # Cancel all pilots.
    pmgr.cancel_pilots()
    pmgr.wait_pilots()

except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    session.close()

# ------------------------------------------------------------------------------

