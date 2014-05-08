import sys
import radical.pilot
import time
import os
import datetime
import urllib2
import json

PWD    = os.path.dirname(os.path.abspath(__file__))
DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
FGCONF = 'file://localhost/%s/config/xsede.json' % PWD

#-------------------------------------------------------------------------------

def pilot_state_cb(pilot, state):
    print "[Callback]: ComputePilot '{0}' state changed to {1}.".format(pilot.uid, state)

def run():
    try:
        compute_units = []
        for k in range(0, 32):
            cu = radical.pilot.ComputeUnitDescription()
            cu.cores = 1
            cu.executable = "/bin/date"
            compute_units.append(cu)

        session = radical.pilot.Session(database_url=DBURL)

        pm = radical.pilot.PilotManager(session=session, resource_configurations=FGCONF)
        pm.register_callback(pilot_state_cb)

        um = radical.pilot.UnitManager(session=session, scheduler=radical.pilot.SCHED_ROUND_ROBIN)

        pd = radical.pilot.ComputePilotDescription()
        pd.resource = "xsede.TRESTLES"
        pd.sandbox = "/home/antontre/re-experiments"
        pd.cores = 32
        pd.runtime = 10
        pd.cleanup = True

        pilot_object = pm.submit_pilots(pd)
        um.add_pilots(pilot_object)
        submitted_units = um.submit_units(compute_units)

        print "Waiting for all compute units to finish..."
        um.wait_units()

        print "  FINISHED"
        pm.cancel_pilots()

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex

#-------------------------------------------------------------------------------

if __name__ == "__main__":

    run()

                                                                                                                                         
