import os
import sys
import time
import radical.pilot as rp

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!


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

    rp_user     = str(os.getenv ("RP_USER",     "merzky"))
    rp_cores    = int(os.getenv ("RP_CORES",    16))
    rp_cu_cores = int(os.getenv ("RP_CU_CORES", 1))
    rp_units    = int(os.getenv ("RP_UNITS",    64))
    rp_runtime  = int(os.getenv ("RP_RUNTIME",  15))
    rp_host     = str(os.getenv ("RP_HOST",     "india.futuregrid.org"))
    rp_queue    = str(os.getenv ("RP_QUEUE",    ""))
    rp_project  = str(os.getenv ("RP_PROJECT",  ""))

    # Create a new session. A session is the 'root' object for all other
    # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
    # well as security crendetials.
    session = rp.Session()
    print "session: %s" % session.uid

    # make jenkins happy
    c         = rp.Context ('ssh')
    c.user_id = rp_user
    session.add_context (c)

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Register our callback with the PilotManager. This callback will get
    # called every time any of the pilots managed by the PilotManager
    # change their state.
    pmgr.register_callback(pilot_state_cb)

    # Define 1-core local pilots that run for 10 minutes and clean up
    # after themself.
    pdescriptions = list()
    for i in range (0, 1) :
        pdesc = rp.ComputePilotDescription()
        pdesc.resource = rp_host
        pdesc.runtime  = rp_runtime
        pdesc.cores    = rp_cores
        pdesc.cleanup  = False
        if rp_queue   : pdesc.queue    = rp_queue
        if rp_project : pdesc.project  = rp_project

        pdescriptions.append(pdesc)

        import pprint
        pprint.pprint (pdesc)


    # Launch the pilots.
    pilots = pmgr.submit_pilots (pdescriptions)
    print "pilots: %s" % pilots

    pmgr.wait_pilots (state=rp.ACTIVE)



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
    cu_descriptions = []

    for unit_count in range(0, rp_units):
        cu = rp.ComputeUnitDescription()
        cu.executable  = "/bin/sleep"
        cu.arguments   = ["60"]
        cu.cores       = rp_cu_cores
        cu.mpi         = True

        import pprint
        pprint.pprint (cu)

        cu_descriptions.append(cu)

    # Combine the ComputePilot, the ComputeUnits and a scheduler via
    # a UnitManager object.
    umgr = rp.UnitManager(
        session=session,
        scheduler=rp.SCHED_ROUND_ROBIN)

    # Register our callback with the UnitManager. This callback will get
    # called every time any of the units managed by the UnitManager
    # change their state.
    umgr.register_callback(unit_state_change_cb)

    # Add the previsouly created ComputePilots to the UnitManager.
    umgr.add_pilots(pilots)

    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    units = umgr.submit_units(cu_descriptions)
    print "units: %s" % umgr.list_units ()

    # Wait for all compute units to reach a terminal state (DONE or FAILED).
    umgr.wait_units()

    if not isinstance (units, list) :
        units=[units]
    
    for unit in units:
        print "* Task %s (executed @ %s) state %s, exit code: %s, started: %s, finished: %s" \
            % (unit.uid, unit.execution_locations, unit.state, unit.exit_code, unit.start_time, unit.stop_time)

    # Close automatically cancels the pilot(s).
    pmgr.cancel_pilots ()
    time.sleep (3)
  
    sid = session.uid

    print "session id: %s" % sid

    # run the stats plotter
    os.system ("bin/radicalpilot-stats -m plot -s %s" % sid) 
    os.system ("cp -v %s.png report/rp.benchmark.png" % sid) 

    session.close ()

