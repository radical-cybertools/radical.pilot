import sagapilot

DBURL  = "mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017"
RCONF  = "https://raw.github.com/saga-project/saga-pilot/devel/configs/xsede.json"

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sagapilot.Session(database_url=DBURL)

        # Add an ssh identity to the session.
        cred = sagapilot.SSHCredential()
        cred.user_id = "tg802352"

        session.add_credential(cred)

        # Add a Pilot Manager with a machine configuration file for FutureGrid
        pmgr = sagapilot.PilotManager(session=session, resource_configurations=RCONF)

        # Define a 32-core on stamped that runs for 15 mintutes and 
        # uses $HOME/sagapilot.sandbox as sandbox directoy. 
        pdesc = sagapilot.ComputePilotDescription()
        pdesc.resource  = "stampede.tacc.utexas.edu"
        pdesc.sandbox   = "/home1/00988/tg802352/sagapilot.sandbox"
        pdesc.runtime   = 15 # minutes
        pdesc.cores     = 32 

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)
        print "Pilot UID       : {0} ".format( pilot.uid )

        # Create a workload of 8 '/bin/sleep' ComputeUnits (tasks)
        compute_units = []

        for unit_count in range(0, 32):
            cu = sagapilot.ComputeUnitDescription()
            #cu.environment = {"NAP_TIME" : "10"}
            cu.executable  = "/bin/date"
            #cu.arguments   = ["$NAP_TIME"]
            cu.cores       = 1
        
            compute_units.append(cu)

        umgr = sagapilot.UnitManager(session=session, scheduler=sagapilot.SCHED_ROUND_ROBIN)
        umgr.add_pilots(pilot)
        
        umgr.submit_units(compute_units)

        # Wait for all compute units to finish.
        umgr.wait_units()

        for unit in umgr.get_units():
            print "UID: {0}, STATE: {1}, START_TIME: {2}, STOP_TIME: {3}".format(
                unit.uid, unit.state, unit.start_time, unit.stop_time)
        
        # Cancel all pilots.
        pmgr.cancel_pilots()

    except sagapilot.SagapilotException, ex:
        print "Error: %s" % ex

    finally:
        # Remove session from database
        session.destroy()
        

