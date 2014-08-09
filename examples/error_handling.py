#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import radical.pilot as rp
import time

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """
    print "[Callback]: ComputePilot '%s' state changed to %s." \
        % (pilot.uid, state)

    if state == rp.FAILED:
        sys.exit (0)
        thread.interrupt_main()


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
        session = rp.Session(database_url=DBURL)

        # Create a new pilot manager.
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Create a new pilot with 128 cores. This will most definetly 
        # fail on 'localhost' because not enough cores are available. 
        pd = rp.ComputePilotDescription()
        pd.resource  = "localhost"
        pd.cores     = 128
        pd.runtime   = 10 

        pilot = pmgr.submit_pilots(pd)
        state = pilot.wait(state=[rp.states.ACTIVE, rp.states.FAILED], timeout=60)

        # If the pilot is in FAILED state it probably didn't start up properly. 
        if state == rp.states.FAILED:
            print pilot.log[-1] # Get the last log message
            return 0
        # The timeout was reached if the pilot state is still FAILED.
        elif state == rp.states.PENDING:
            print "Timeout..."
            return 1
        # If the pilot is not in FAILED or PENDING state, it is probably running.
        else:
            print "Pilot in state '%s'" % state
            # Since the pilot is running, we can cancel it now.
            # We should not hve gooten that far.
            pilot.cancel()
            return 1

    except Exception as ex:
        print "Error: %s" % ex
        return 0

    # Oh, we should have seen an error!
    return -1

#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    sys.exit(synchronous_error_handling())

