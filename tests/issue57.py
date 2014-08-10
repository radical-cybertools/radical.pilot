import sys
import radical.pilot as rp
import time
import numpy
import os

#-------------------------------------------------------------------------------

if __name__ == "__main__":

    print "Test: Adding CUs in bulks"

    for i in [8, 16]:

        time_to_submission = []
        time_from_sub_to_start = []
        runtime = []
        for j in range(0, 1):

            session = rp.Session()
            pm = rp.PilotManager(session=session)
            um = rp.UnitManager(session=session, scheduler=rp.SCHED_ROUND_ROBIN) 

            compute_units = []
            print "submitting %d CUs to pilot" % ( i*2 )
            for k in range(0, i*2):
                cu = rp.ComputeUnitDescription()
                cu.cores = 1
                cu.executable = "/bin/date"
                compute_units.append(cu)

            pilot = []

            pd = rp.ComputePilotDescription()
            pd.resource = "hotel.futuregrid.org"
            pd.cores = i
            pd.runtime = 10
            #pd.cleanup = True
            pilot.append(pd)

            pilot_object = pm.submit_pilots(pilot)

            if pilot_object.state in [rp.FAILED]:
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

#-------------------------------------------------------------------------------

