__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import sinon
import time

PWD    = os.path.dirname(os.path.abspath(__file__))

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
FGCONF = 'file://localhost/%s/../../configs/futuregrid.json' % PWD

#-------------------------------------------------------------------------------
#
def demo_milestone_03():
    """Demo for Milestone 3: 
        * submit 2 Pilot to india and 2 to sierra
        * run 10 bulks of 10 CUs (CUs vary in runtime)
        * after 5 bulks: disconnect / reconnect
        * state changes for pilots and CUs are delivered via notifications
        * performance for above is measured and reported routinely
    """
    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sinon.Session(database_url=DBURL)

        # Add a Pilot Manager with a machine configuration file for FutureGrid
        pmgr = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        # Submit a 16-core pilot to india.futuregrid.org
        pd_hotel = sinon.ComputePilotDescription()
        pd_hotel.resource          = "futuregrid.HOTEL"
        pd_hotel.working_directory = "/N/u/oweidner/scratch/sinon"
        pd_hotel.cores             = 16
        pd_hotel.run_time          = 10 # minutes

        # Submit a 16-core pilot to india.futuregrid.org
        pd_india = sinon.ComputePilotDescription()
        pd_india.resource          = "futuregrid.HOTEL"
        pd_india.working_directory = "/N/u/oweidner/scratch/sinon"
        pd_india.cores             = 16
        pd_india.run_time          = 10 # minutes

        print "* Submitting pilot to '%s'..." % \
            [pd_hotel.resource, pd_india.resource]
        pilots = pmgr.submit_pilots([pd_hotel, pd_india])

        time.sleep(60)

        # Cancel all pilots.
        pmgr.cancel_pilots()
        return 0

    except sinon.SinonException, ex:
        print "Error: %s" % ex
        return -1

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(demo_milestone_03())

