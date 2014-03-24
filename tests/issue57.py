import sys
import sagapilot
import time
import numpy
import os

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("SAGAPILOT_DBURL")
if DBURL is None:
    print "ERROR: SAGAPILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

# RCONF points to the resource configuration files. Read more about resource 
# configuration files at http://saga-pilot.readthedocs.org/en/latest/machconf.html
RCONF  = ["https://raw.github.com/radical-cybertools/radical.pilot/devel/configs/xsede.json",
          "https://raw.github.com/radical-cybertools/radical.pilot/devel/configs/futuregrid.json"]

#-------------------------------------------------------------------------------

def cu_bulk_submit_test():

    print "Test: Adding CUs in bulks"

    try:
        for i in [8, 16]:

            time_to_submission = []
            time_from_sub_to_start = []
            runtime = []
            for j in range(0, 1):

                session = sagapilot.Session(database_url=DBURL)
                pm = sagapilot.PilotManager(session=session, resource_configurations=RCONF)
                um = sagapilot.UnitManager(session=session, scheduler=sagapilot.SCHED_ROUND_ROBIN) 

                compute_units = []
                print "submitting %d CUs to pilot" % ( i*2 )
                for k in range(0, i*2):
                    cu = sagapilot.ComputeUnitDescription()
                    cu.cores = 1
                    cu.executable = "/bin/date"
                    compute_units.append(cu)

                pilot = []

                pd = sagapilot.ComputePilotDescription()
                pd.resource = "hotel.futuregrid.org"
                pd.cores = i
                pd.runtime = 10
                #pd.cleanup = True
                pilot.append(pd)

                pilot_object = pm.submit_pilots(pilot)

                if pilot_object.state in [sagapilot.states.FAILED]:
                    print " * [ERROR] Pilot failed"
                else:
                    print " * [OK] Pilot %s submitted successfully" % (pilot_object)

                um.add_pilots(pilot_object)

                print "submitting CUS %s" % compute_units
                um.submit_units(compute_units)

                print "* Waiting for all compute units to finish..."
                um.wait_units()

                print "  FINISHED"
                pm.cancel_pilots(pilot_object.uid)       
                time.sleep(1)

    except sagapilot.SagapilotException, ex:
        print "Error: %s" % ex

    try:        
        session.destroy()
    except sagapilot.SagapilotException, ex:
        print "Error: %s" % ex

#-------------------------------------------------------------------------------

if __name__ == "__main__":

    cu_bulk_submit_test()