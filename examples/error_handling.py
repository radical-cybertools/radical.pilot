#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!


#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):
    """ this callback is invoked on all pilot state changes """

    # Callbacks happen in a different thread than the main application thread --
    # they are truly asynchronous.  That means, however, that a 'sys.exit()'
    # will not end the application, but will end the thread (in this case the
    # pilot_manager_controller thread).  For that reason we wrapped all threads
    # in their own try/except clauses, and then translate the `sys.exit()` into an
    # 'thread.interrupt_main()' call -- this will raise a 'KeyboardInterrupt' in
    # the main thread which can be interpreted by your application, for example
    # to initiate a clean shutdown via `session.close()` (see code later below.)
    # The same `KeyboardShutdown` will also be raised when you interrupt the
    # application via `^C`.
    #
    # Note that other error handling semantics is available, depending on your
    # application's needs.  The application could for example spawn
    # a replacement pilot at this point, or reduce the number of compute units
    # to match the remaining set of pilots.

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        print 'Pilot failed -- ABORT!  ABORT!  ABORT!'
        print pilot.log[-1] # Get the last log message
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):
    """ this callback is invoked on all unit state changes """

    # The principle for unit state callbacks is exactly the same as for the
    # pilot state callbacks -- only that they are invoked by the unit manager on
    # changes of compute unit states.  
    #
    # The example below does not really create any ComputeUnits, we only include
    # the callback here for documentation on the principles of error handling.
    #
    # Note that other error handling semantics is available, depending on your
    # application's needs.  The application could for example spawn replacement
    # compute units, or spawn a pilot on a different resource which might be
    # better equipped to handle the unit payload.

    print "[Callback]: ComputeUnit '%s' state: %s." % (unit.uid, state)

    if state == rp.FAILED:
        print 'Unit failed -- ABORT!  ABORT!  ABORT!'
        print 'stderr: %s' % unit.stderr # Get the unit's stderr
        sys.exit (1)


#-------------------------------------------------------------------------------
#
if __name__ == "__main__":
    # This example shows how simple error handling can be implemented 
    # synchronously using blocking wait() calls.
    #
    # The code launches a pilot with 128 cores on 'localhost'. Unless localhost
    # has 128 or more cores available, this is bound to fail. This example shows
    # how this error can be caught and handled. 

    # we can optionally pass session name to RP
    if len(sys.argv) > 1:
        session_name = sys.argv[1]
    else:
        session_name = None

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session(name=session_name)
    print "session id: %s" % session.uid

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # do pilot thingies
        umgr = rp.UnitManager(session=session)
        pmgr = rp.PilotManager(session=session)

        # Register our callbacks with the managers. The callbacks will get
        # called every time any of the pilots or units change their state 
        # -- in particular also on failing ones.
        umgr.register_callback(unit_state_cb)
        pmgr.register_callback(pilot_state_cb)

        # Create a local pilot.
        pd = rp.ComputePilotDescription()
        pd.resource  = "local.localhost"
        pd.cores     = 1
        pd.runtime   = 60

        pilot = pmgr.submit_pilots(pd)
        umgr.add_pilots(pilot)

        # we submit one compute unit which will just fail
        cud = rp.ComputeUnitDescription()
        cud.executable = '/bin/fail'

        # submit the unit...
        cu = umgr.submit_units(cud)

        # ...and wait for it's successfull 'completion', ie. forever
        state = umgr.wait_units (state=[rp.DONE])

    except Exception as e:
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print "need to exit now: %s" % e

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print "closing session"
        session.close ()

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


#-------------------------------------------------------------------------------

