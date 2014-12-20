import os
import sys
import datetime
import radical.pilot as rp

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scenes!


#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state changed to %s at %s." % (pilot.uid, state, datetime.datetime.now())

    if  state == rp.FAILED:
        sys.exit (1)

#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit '%s' state changed to %s at %s." % (unit.uid, state, datetime.datetime.now())

    if state in [rp.FAILED] :
        print "stdout: %s" % unit.stdout
        print "stderr: %s" % unit.stderr


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # Create a new session. A session is the 'root' object for all other
    # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
    # well as security credentials.
    session = rp.Session()

    # Add an ssh identity to the session.
    c = rp.Context('ssh')
    #c.user_id = "alice"
    #c.user_pass = "ILoveBob!" # Ideally use SSH public key authentication!
    session.add_context(c)

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)
    pdescs = list()

    for i in range (1) :
        # Define a 32-core on stampede that runs for 15 minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = rp.ComputePilotDescription()
        pdesc.resource  = "stfc.joule"
        pdesc.runtime   = 5 # minutes
        pdesc.cores     = 16
        pdesc.queue     = "prod"

        pdescs.append (pdesc)

    # Launch the pilot.
    pilots = pmgr.submit_pilots(pdescs)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(
        session=session,
        scheduler=rp.SCHED_DIRECT_SUBMISSION)

    # Register our callback with the UnitManager. This callback will get
    # called every time any of the units managed by the UnitManager
    # change their state.
    umgr.register_callback(unit_state_cb)

    # Add the previously created ComputePilot to the UnitManager.
    umgr.add_pilots(pilots)

    cuds = list()

    for unit_count in range(0, 4):
        cud = rp.ComputeUnitDescription()
        cud.executable    = "python"
        cud.arguments     = ["-c", "import os; print 'Hello World, %s.'" % os.getenv('USER')]
        cud.cores         = 16
        cuds.append(cud)

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    units = umgr.submit_units(cuds)

    # Wait for all compute units to reach a terminal state (DONE or FAILED).
    umgr.wait_units()

    for unit in units:
        print "* Unit %s (executed @ %s) state: %s, exit code: %s, started: %s, finished: %s, output: %s" \
            % (unit.uid, unit.execution_locations, unit.state, unit.exit_code, unit.start_time, unit.stop_time,
               unit.stdout)

    # Close automatically cancels the pilot(s).
    session.close()

# ------------------------------------------------------------------------------
