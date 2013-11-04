__copyright__ = "Copyright 2013, The RADICAL Group at Rutgers University"
__license__   = "MIT"

import sys
import sinon

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
FGCONF = 'https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json'

#-------------------------------------------------------------------------------
#
def demo_milestone_02():
    """Demo for Milestone 2: Submit a single pilot to FutureGrid an execute 
    O(10) work units. 
    """
    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sinon.Session(database_url=DBURL)

        # Add a Pilot Manager with a machine configuration file for FutureGrid
        pm = sinon.PilotManager(session=session, machine_configurations=FGCONF)

        # Submit a 16-core pilot to sierra.futuregrid.org
        pd = sinon.ComputePilotDescription()
        pd.resource = "futuregrid.SIERRA"
        pd.cores = 16
        sierra_pilot = pm.submit_pilots(pd)

    except sinon.SinonException, ex:
        print "Error: %s" % ex


#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(demo_milestone_02())