"""Demo for Milestone 3: 
    * submit 2 Pilot to india and 2 to sierra
    * run 10 bulks of 10 CUs (CUs vary in runtime)
    * after 5 bulks: disconnect / reconnect
    * state changes for pilots and CUs are delivered via notifications
    * performance for above is measured and reported routinely
"""

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
def demo_milestone_03_part_1():
    """PART 1: Create two 16-core pilots on hotel and india, submit 16 bulks 
    of 64 compute unites and disconnect. 
    """
    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sinon.Session(database_url=DBURL)

        # Add a Pilot Manager with a machine configuration file for FutureGrid
        pmgr = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        # Submit a 16-core pilot to hotel.futuregrid.org
        pd_hotel = sinon.ComputePilotDescription()
        pd_hotel.resource          = "futuregrid.HOTEL"
        pd_hotel.working_directory = "/N/u/oweidner/scratch/sinon"
        pd_hotel.cores             = 16
        pd_hotel.run_time          = 10 # minutes

        # Submit a 16-core pilot to india.futuregrid.org
        pd_india = sinon.ComputePilotDescription()
        pd_india.resource          = "futuregrid.INDIA"
        pd_india.working_directory = "/N/u/oweidner/sinon"
        pd_india.cores             = 16
        pd_india.run_time          = 10 # minutes

        print "* Submitting pilot to '%s'..." % \
            [pd_hotel.resource, pd_india.resource]
        pilots = pmgr.submit_pilots([pd_hotel, pd_india])

        umgr = sinon.UnitManager(session=session, scheduler="round_robin")
        #umgr.add_pilots(pd_hotel, pd_india)

        return (session.uid, pmgr.uid, umgr.uid)

    except sinon.SinonException, ex:
        print "Error: %s" % ex
        sys.exit(255)

#-------------------------------------------------------------------------------
#
def demo_milestone_03_part_2(session_id, pmgr_id, umgr_id):
    """PART 2: Reconenct
    """
    try:
        # Re-connect to the previously created session via its ID.
        session = sinon.Session(session_uid=session_id, database_url=DBURL)
        print "* Reconnected to session with session ID %s" % session.uid

        pmgr = sinon.PilotManager.get(session=session, pilot_manager_id=pmgr_id)
        #umgr = sinon.UnitManager.get(session=session, unit_manager_id=umgr_id)

        pmgr.cancel_pilots()

    except sinon.SinonException, ex:
        print "Error: %s" % ex
        sys.exit(255)

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    session_id, pmgr_id, umgr_id = demo_milestone_03_part_1()

    time.sleep(30)

    demo_milestone_03_part_2(session_id, pmgr_id, umgr_id)
    
    sys.exit(0)



