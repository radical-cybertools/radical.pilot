import sys
import sinon
import time
import numpy
import os
import datetime
import matplotlib.pyplot as plt


PWD = os.path.dirname(os.path.abspath(__file__))

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("SAGAPILOT_DBURL")
if DBURL is None:
    print "ERROR: SAGAPILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

FGCONF = 'file://localhost/%s/configs/futuregrid.json' % PWD

#-------------------------------------------------------------------------------

def pilot_bulk_submit_test_real():

    print "Test: Adding Pilots in bulks"
    try:
        session = sinon.Session(database_url=DBURL)
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)
        um = sinon.UnitManager(session=session, scheduler=sinon.SCHED_ROUND_ROBIN)  

        for i in [1,2,4,8]:
            for j in range(0, 2):
                
                pilots = []
                for k in range(0, i):
                    pd = sinon.ComputePilotDescription()
                    pd.resource = "futuregrid.HOTEL"
                    pd.working_directory = "/N/u/oweidner/sinon-performance"
                    pd.cores = 8
                    pd.run_time = 10
                    pd.cleanup = True
                    pilots.append(pd)
                
                pilot_objects = pm.submit_pilots(pilots)

                if i > 1:
                    for pilot in pilot_objects:
                        if pilot.state in [sinon.states.FAILED]:
                            print " * [ERROR] Pilot %s failed: %s." % (pilot, pilot.log[-1])
                        else:
                            print " * [OK] Pilot %s submitted successfully: %s" % (pilot, pilot.log[-1])
                else:
                    if pilot_objects.state in [sinon.states.FAILED]:
                        print " * [ERROR] Pilot %s failed: %s." % (pilot_objects, pilot_objects.log[-1])
                    else:
                        print " * [OK] Pilot %s submitted successfully: %s" % (pilot_objects, pilot_objects.log[-1])

                compute_units = []
                for unit_count in range(0, i):
                    cu = sinon.ComputeUnitDescription()
                    cu.cores = 1
                    cu.executable = "/bin/date"
                    compute_units.append(cu)

                um.add_pilots(pilot_objects)
                um.submit_units(compute_units)
                print "* Waiting for all compute units to finish..."
                um.wait_units()

                print "  FINISHED"
                pm.cancel_pilots()

    except sinon.SinonException, ex:
        print "Error: %s" % ex

    try:        
        session.destroy()
    except sinon.SinonException, ex:
        print "Error: %s" % ex

#-------------------------------------------------------------------------------

if __name__ == "__main__":
    
    session_uid = pilot_bulk_submit_test_real()

