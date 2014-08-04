import os
import sys
import time
import radical.pilot

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!
#


# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to http://docs.mongodb.org.
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)


#------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):
    """ 
    this callback is called when the size of the unit managers wait_queue
    changes.
    """
    print "[Callback]: UnitManager  '%s' wait_queue_size changed to %s." \
        % (umgr.uid, wait_queue_size)

    pilots = umgr.get_pilots ()
    for pilot in pilots :
        print "pilot %s: %s" % (pilot.uid, pilot.state)

    if  wait_queue_size == 0 :
        for pilot in pilots :
            if  pilot.state in [radical.pilot.PENDING_LAUNCH,
                                radical.pilot.LAUNCHING     ,
                                radical.pilot.PENDING_ACTIVE] :
                print "cancel pilot %s" % pilot.uid
                umgr.remove_pilot (pilot.uid)
                pilot.cancel ()

#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """
    print "[Callback]: ComputePilot '{0}' state changed to {1}.".format(
        pilot.uid, state)
    if state == radical.pilot.states.FAILED:
        print "            Log: %s" % pilot.log[-1]

#------------------------------------------------------------------------------
#
def unit_state_change_cb(unit, state):
    """unit_state_change_cb() is a callback function. It gets called very
    time a ComputeUnit changes its state.
    """
    print "[Callback]: ComputeUnit  '{0}' state changed to {1}.".format(
        unit.uid, state)
    if state == radical.pilot.states.FAILED:
        print "            Log: %s" % unit.log[-1]

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security credentials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a 4-core local pilot that runs for 10 minutes and cleans up
        # after itself.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = "localhost"
        pdesc.runtime  = 10 # minutes
        pdesc.cores    = 4
        pdesc.cleanup  = True

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        pdesc2 = radical.pilot.ComputePilotDescription()
        pdesc2.resource = "localhost"
        pdesc2.runtime  = 10 # minutes
        pdesc2.cores    = 4
        pdesc2.cleanup  = True

        # Launch the pilot.
        pilot2 = pmgr.submit_pilots(pdesc)

        # Create a workload of 8 ComputeUnits (tasks). Each compute unit
        # uses /bin/cat to concatenate two input files, file1.dat and
        # file2.dat. The output is written to STDOUT. cu.environment is
        # used to demonstrate how to set environment variables within a
        # ComputeUnit - it's not strictly necessary for this example. As
        # a shell script, the ComputeUnits would look something like this:
        #
        #    export INPUT1=file1.dat
        #    export INPUT2=file2.dat
        #    /bin/cat $INPUT1 $INPUT2
        #
        cuds = []
        for unit_count in range(0, 8):
            cud = radical.pilot.ComputeUnitDescription()
            cud.executable    = "/bin/bash"
            cud.environment   = {'INPUT1': 'file1.dat', 'INPUT2': 'file2.dat'}
            cud.arguments     = ["-l", "-c", "cat $INPUT1 $INPUT2"]
            cud.cores         = 1
            cud.input_staging = ['file1.dat', 'file2.dat']

            cuds.append(cud)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_BACKFILLING)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_change_cb, radical.pilot.UNIT_STATE)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots([pilot, pilot2])

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cuds)

        # Register also a callback which tells us when all units have been
        # assigned to pilots
        umgr.register_callback(wait_queue_size_cb,   radical.pilot.WAIT_QUEUE_SIZE)

        # Wait for all compute units to reach a terminal state (DONE or FAILED).
        umgr.wait_units()

        print 'units all done'
        print '----------------------------------------------------------------------'

        for unit in units :
            unit.wait ()

        for unit in units:
            print "* Task %s (executed @ %s) state %s, exit code: %s, started: %s, finished: %s, stdout: %s" \
                % (unit.uid, unit.execution_locations, unit.state, unit.exit_code, unit.start_time, unit.stop_time, unit.stdout)

        # Close automatically cancels the pilot(s).
        pmgr.cancel_pilots ()
        print session.uid
        session.close(delete=False)
        sys.exit(0)

    except radical.pilot.PilotException, ex:
        # Catch all exceptions and exit with and error.
        print "Error during execution: %s" % ex
        sys.exit(1)

