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
def error_handling_1():
    """Short description.
    """
    try:
        # Create a new session. A session is a set of Pilot Managers
        # and Unit Managers (with associated Pilots and ComputeUnits).
        session = sinon.Session(database_url=DBURL)

        # Add a Pilot Manager with a machine configuration file for FutureGrid
        pm = sinon.PilotManager(session=session, resource_configurations=FGCONF)

        pd = sinon.ComputePilotDescription()
        pd.resource          = "localhost"
        pd.working_directory = "/tmp/sinon"
        pd.cores             = 128
        pd.run_time          = 10 

        pilot = pm.submit_pilots(pd)
        state = pilot.wait(state=[sinon.states.RUNNING, sinon.states.FAILED])

        if state == sinon.states.FAILED:
            print pilot.state_details[-1] # Get the last log message
            return 1

        else:
            return 0

    except sinon.SinonException, ex:
        print "Error: %s" % ex
        return -1

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(error_handling_1())

