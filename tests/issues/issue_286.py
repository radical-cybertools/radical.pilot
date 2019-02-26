#!/usr/bin/env python


import sys
import radical.pilot as rp

# ##############################################################################
# #133: CUs fail when radical pilot run on/as localhost
# ##############################################################################

#------------------------------------------------------------------------------
#
# Create a new session. A session is the 'root' object for all other
# RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
# well as security crendetials.
session = rp.Session()

try:

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Define a 2-core local pilot that runs for 10 minutes.
    pdesc = rp.ComputePilotDescription()
    pdesc.resource = "local.localhost"
    pdesc.runtime  = 5
    pdesc.cores    = 1

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    cu = rp.ComputeUnitDescription()
    cu.executable    = "cat"
    cu.arguments     = ["issue_286.txt"]
    cu.input_staging =  "issue_286.txt"
    cu.cores = 1

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(session=session,
                          scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

    # Add the previsouly created ComputePilot to the UnitManager.
    umgr.add_pilots(pilot)

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    unit = umgr.submit_units(cu)

    # Wait for all compute units to finish.
    umgr.wait_units()

    print "unit.state : %s" % unit.state
    print "unit.stdout: %s" % unit.stdout
    print "unit.stderr: %s" % unit.stderr

except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    session.close()

# ------------------------------------------------------------------------------

