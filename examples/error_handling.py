#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import radical.pilot as rp
import time

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!


#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

    # callbacks happen in a different thread than the main application thread --
    # they are truly asynchronous.  That means, however, that a 'sys.exit()'
    # will not end the application, but will end the thread (in this case the
    # pilot_manager_controller thread).  For that reason we wrapped all threads
    # in their own try/except clauses, and the translate that exception into an
    # 'thread.interrupt_main()' call -- this will raise a 'KeyboardInterrupt' in
    # the main thread which in turn can be caughte that exception into an
    # 'thread.interrupt_main()' call -- this will raise a 'KeyboardInterrupt'
    # exception in the main thread, which in turn can then be caught for clean
    # shutdown (see code later below.)

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if  state == rp.FAILED :
        print 'Pilot failed -- ABORT!  ABORT!  ABORT!'
        print pilot.log[-1] # Get the last log message
        sys.exit (1)


#-------------------------------------------------------------------------------
#
if __name__ == "__main__":

    """
    This example shows how simple error handling can be implemented 
    synchronously using blocking wait() calls.

    The code launches a pilot with 128 cores on 'localhost'. Unless localhost
    has 128 or more cores available, this is bound to fail. This example shows
    how this error can be caught and handled. 
    """

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    
    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try :

        # do pilot thingies
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state -- in particular also on failing pilots.
        pmgr.register_callback(pilot_state_cb)

        # Create a local pilot with a million cores. This will most likely
        # fail as not enough cores will be available.  That means the pilot will
        # go quickly into failed state, and trigger the callback from above.
        pd = rp.ComputePilotDescription()
        pd.resource  = "localhost"
        pd.cores     = 1000000
        pd.runtime   = 60

        pilot = pmgr.submit_pilots(pd)

        # this will basically wait forever (the pilot won't reach DONE state...
        state = pilot.wait (state=[rp.DONE])

    except Exception as e :
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e

    except (KeyboardInterrupt, SystemExit) as e :
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some
        # reason).
        print "need to exit now: %s" % e

    finally :
        # always clean up the session, no matter if we caught an exception or
        # not.
        print "closing session"
        session.close ()

        # the above is equivalent to
        # session.close (cleanup=True, terminate=True)


#-------------------------------------------------------------------------------

