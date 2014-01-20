#!/usr/bin/env python

""" LINK TO TUTORIAL PAGE
"""

__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import sinon
import time

PWD    = os.path.dirname(os.path.abspath(__file__))
DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
FGCONF = 'file://localhost/%s/../../configs/futuregrid.json' % PWD

#-------------------------------------------------------------------------------
# Change these according to your needs 
CFG_USERNAME    = "oweidner"
CFG_RESOURCE    = "localhost"    
CFG_WORKING_DIR = "/tmp/sinon/"

#-------------------------------------------------------------------------------
#
def synchronous_error_handling():
    """This example shows how simple error handling can be implemented 
    synchronously using blocking wait() calls.

    The code launches a pilot with 128 cores on 'localhost'. Unless localhost
    has 128 or more cores available, this is bound to fail. This example shows
    how this error can be caught and handled. 
    """
    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sinon.Session(database_url=DBURL)

        # Create a new pilot manager.
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        # Create a new pilot with 128 cores. This will most definetly 
        # fail on 'localhost' because not enough cores are available. 
        pd = sinon.ComputePilotDescription()
        pd.resource          = "localhost"
        pd.working_directory = "/tmp/sinon"
        pd.cores             = 128
        pd.run_time          = 10 

        pilot = pm.submit_pilots(pd)
        state = pilot.wait(state=[sinon.states.RUNNING, sinon.states.FAILED], timeout=60)

        # If the pilot is in FAILED state it probably didn't start up properly. 
        if state == sinon.states.FAILED:
            print pilot.state_details[-1] # Get the last log message
            return 1
        # The timeout was reached if the pilot state is still FAILED.
        elif state == sinon.states.PENDING:
            print "Timeout..."
            return 1
        # If the pilot is not in FAILED or PENDING state, it is probably running.
        else:
            print "Pilot in state '%s'" % state
            # Since the pilot is running, we can cancel it now.
            pilot.cancel()
            return 0

    except sinon.SinonException, ex:
        print "Error: %s" % ex
        return -1

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(synchronous_error_handling())

