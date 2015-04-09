#!/usr/bin/env python


import os
import sys
import radical.pilot as rp

# ##############################################################################
# #165: proper handling of quotes in arguments for the multicore agent
# ##############################################################################

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
# RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
# well as security contexts.
session = rp.Session()

try:

    # prepare some input files for the compute units
    os.system ('hostname > file1.dat')
    os.system ('date     > file2.dat')

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define a X-core on stamped that runs for N minutes and
    # uses $HOME/radical.pilot.sandbox as sandbox directoy. 
    pdesc = rp.ComputePilotDescription()
    pdesc.resource = "local.localhost"
    pdesc.runtime  = 5 # N minutes
    pdesc.cores    = 1 # X cores
    pdesc.cleanup  = True

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

    # Add the previously created ComputePilot to the UnitManager.
    umgr.add_pilots(pilot)


    cud = rp.ComputeUnitDescription()
    ###
    ### Arguments are all treated as strings and don't need special quoting in the CUD.
    ###
    cud.executable = "/bin/bash"
    cud.arguments = ["-l", "-c", "cat ./file1.dat ./file2.dat > result.dat"]
    ###
    ### In the backend, arguments containing spaces will get special treatment, so that they
    ### remain intact as strings.
    ###
    ### This CUD will thus be executed as: /bin/bash -l -c "cat ./file1.dat ./file2.dat > result.dat"
    ###
    cud.input_staging  = ['file1.dat', 'file2.dat']
    cud.output_staging = ['result.dat']

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    unit = umgr.submit_units(cud)

    # Wait for all compute units to reach a terminal state (DONE or FAILED).
    umgr.wait_units()

    print "* Task %s - state: %s, exit code: %s, started: %s, finished: %s, stdout: %s" \
        % (unit.uid, unit.state, unit.exit_code, unit.start_time, unit.stop_time, "n.a.")
    assert (unit.state == rp.DONE)

    pmgr.cancel_pilots()
    pmgr.wait_pilots()

except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    session.close()

    # delete the test data files
    os.system ('rm -f file1.dat')
    os.system ('rm -f file2.dat')
    os.system ('rm -f result.dat')



#-------------------------------------------------------------------------------

