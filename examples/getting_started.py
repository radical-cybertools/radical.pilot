__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import sagapilot


import os 
PWD    = os.path.dirname(os.path.abspath(__file__))
DBURL  = "mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017"
FGCONF = 'file://localhost/%s/../../configs/futuregrid.json' % PWD

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sagapilot.Session(database_url=DBURL)

        print "S UID           : {0} ".format(session.uid)
        print "S Crentials     : {0} ".format(session.list_credentials())
        print "S UnitManagers  : {0} ".format(session.list_unit_managers())
        print "S PilotManagers : {0} ".format(session.list_pilot_managers())

        # Add a Pilot Manager with a machine configuration file for FutureGrid
        pmgr = sagapilot.PilotManager(session=session)
        print "PM UID          : {0} ".format( pmgr.uid )
        print "PM Pilots       : {0} ".format( pmgr.list_pilots() )

        # Define a 2-core local pilot in /tmp/sagapilot.sandbox that runs 
        # for 10 minutes.
        pdesc = sagapilot.ComputePilotDescription()
        pdesc.resource  = "localhost"
        pdesc.sandbox   = "/tmp/sagapilot.sandbox"
        pdesc.runtime   = 15 # minutes | https://github.com/saga-project/saga-python/issues/300
        pdesc.cores     = 2 

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # state = p1.wait(state=[sinon.states.RUNNING, sinon.states.FAILED])

        # # If the pilot is in FAILED state it probably didn't start up properly. 
        # if state == sinon.states.FAILED:
        #     print "  [ERROR] Pilot %s failed: %s." % (p1, p1.state_details[-1])
        #     sys.exit(-1)
        # else:
        #     print "  [OK]    Pilot %s submitted successfully: %s." % (p1, p1.state_details[-1])

        # # Create a workload of 64 '/bin/date' compute units
        # compute_units = []
        # for unit_count in range(0, 16):
        #     cu = sinon.ComputeUnitDescription()
        #     cu.cores = 1
        #     cu.executable = "/bin/date"
        #     compute_units.append(cu)


        # # Combine the pilot, the workload and a scheduler via 
        # # a UnitManager.
        # um = sinon.UnitManager(session=session, scheduler="round_robin")
        # um.add_pilots(p1)
        # um.submit_units(compute_units)

        # unit_list = um.list_units()
        # print "* Submitted %s compute units: %s" % (len(unit_list), unit_list)

        # # Wait for all compute units to finish.
        # print "* Waiting for all compute units to finish..."
        # um.wait_units()
        # print "  FINISHED"

        # # Cancel all pilots.
        # pm.cancel_pilots()

        # return 0

    except sagapilot.SagapilotException, ex:
        print "Error: %s" % ex
        

