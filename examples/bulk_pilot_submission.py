__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import sinon
import time

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
FGCONF = 'https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json'

#-------------------------------------------------------------------------------
#
def bulk_pilot_submission():
    """Shows how multiple pilots are submitted in bulk and how 
    error handling should be implemented. 

    The results should look something like this::
    
        * [ERROR] Pilot {'type': 'pilot', 'id': '528d13296bf88b12e91e19e0'} failed: No entry found for resource key 'futuregrid.DOES_NOT_EXIST' in resource configuration..
        * [OK]    Pilot {'type': 'pilot', 'id': '528d13296bf88b12e91e19df'} submitted successfully: Pilot Job successfully submitted with JobID '[pbs+ssh://sierra.futuregrid.org/.]-[705456]'
        * [ERROR] Pilot {'type': 'pilot', 'id': '528d13296bf88b12e91e19de'} failed: Pilot Job submission failed: 'invalid dir '/N/u/merzky/sinon/pilot-528d13296bf88b12e91e19de': mkdir: cannot create directory `/N/u/merzky/sinon/pilot-528d13296bf88b12e91e19de': Permission denied
    """
    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sinon.Session(database_url=DBURL)

        # Add a Pilot Manager with a machine configuration file for FutureGrid
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        # Submit a 16-core pilot to india.futuregrid.org -- this should work
        pd_1 = sinon.ComputePilotDescription()
        pd_1.resource = "futuregrid.INDIA"
        pd_1.cores = 16
        pd_1.cleanup = True

        # Submit a 16-core pilot to india.futuregrid.org -- this should fail
        pd_2 = sinon.ComputePilotDescription()
        pd_2.resource = "futuregrid.SIERRA"
        pd_2.working_directory = "sftp://sierra.futuregrid.org/N/u/oweidner/sinon/"
        pd_2.cores = 16
        pd_2.cleanup = True

        # Submit a 16-core pilot to india.futuregrid.org -- this should fail
        pd_3 = sinon.ComputePilotDescription()
        pd_3.resource = "futuregrid.DOES_NOT_EXIST"
        pd_3.cores = 16
        pd_3.cleanup = True

        # Submit pilots and check for errors
        pilots = pm.submit_pilots([pd_1, pd_2, pd_3])
        for pilot in pilots:
            if pilot.state in [sinon.states.FAILED]:
                print " * [ERROR] Pilot %s failed: %s." % (pilot, pilot.state_details[-1])
                # Add some smart fault tolerance mechanism here! 
            else:
                print " * [OK]    Pilot %s submitted successfully: %s" % (pilot, pilot.state_details[-1])

        # Cancel all pilots.
        pm.cancel_pilots()

    except sinon.SinonException, ex:
        print "Error: %s" % ex
        return -1

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(bulk_pilot_submission())
