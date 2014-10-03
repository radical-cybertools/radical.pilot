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
def unit_state_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try :
        rp_cores    = int(os.getenv ("RP_CORES",    16))
        rp_cu_cores = int(os.getenv ("RP_CU_CORES", 1))
        rp_units    = int(os.getenv ("RP_UNITS",    rp_cores * 3 * 3 * 2)) # 3 units/core/pilot
        rp_runtime  = int(os.getenv ("RP_RUNTIME",  15))
        rp_host     = str(os.getenv ("RP_HOST",     "xsede.stampede"))
        rp_queue    = str(os.getenv ("RP_QUEUE",    ""))
        rp_project  = str(os.getenv ("RP_PROJECT",  "TG-MCB090174"))

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
        for i in [1, 2, 3] :
            pdesc = rp.ComputePilotDescription()
            pdesc.resource = rp_host
            pdesc.runtime  = rp_runtime
            pdesc.cores    = i*rp_cores
            pdesc.cleanup  = False
            if rp_queue   : pdesc.queue    = rp_queue
            if rp_project : pdesc.project  = rp_project

            pdescriptions.append(pdesc)


        # Launch the pilots.
        pilots = pmgr.submit_pilots (pdescriptions)
        print "pilots: %s" % pilots

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager(
            session=session,
            scheduler=rp.SCHED_BACKFILLING)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)

        # Add the previsouly created ComputePilots to the UnitManager.
        umgr.add_pilots(pilots)


        # Create a workload of n ComputeUnits.
        cu_descriptions = []

        for unit_count in range(0, rp_units):
            cu = rp.ComputeUnitDescription()
            cu.executable  = "/bin/sleep"
            cu.arguments   = ["30"]
            cu.cores       = rp_cu_cores
            cu.mpi         = True

            import pprint
            pprint.pprint (cu)

            cu_descriptions.append(cu)

        # Submit the ComputeUnit descriptions to the UnitManager. This will trigger
        # the selected scheduler to start assigning the units to the pilots.
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

    except Exception as e :
        print "exception: %s" % e

    except (SystemExit, KeyboardInterrupt) as e :
        print "exciting: %s" % e

    finally :
        print "shut down session %s" % session.uid
        session.close (cleanup=True)

