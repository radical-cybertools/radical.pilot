#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import random

import radical.pilot as rp

# READ: The RADICAL-Pilot documentation:
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if
# you want to see what happens behind the scences!


# ------------------------------------------------------------------------------
#
def pilot_state_cb(pilots):
    """
    this callback is invoked on all pilot state changes
    """

    # Callbacks happen in a different thread than the main application thread --
    # they are truly asynchronous.  That means, however, that a 'sys.exit()'
    # will not end the application, but will end the thread (in this case the
    # pilot_manager_controller thread).  For that reason we wrapped all threads
    # in their own try/except clauses, and then translate the `sys.exit()` into
    # an 'thread.interrupt_main()' call -- this will raise a 'KeyboardInterrupt'
    # in the main thread which can be interpreted by your application, for
    # example to initiate a clean shutdown via `session.close()` (see code later
    # below.) The same `KeyboardShutdown` will also be raised when you interrupt
    # the application via `^C`.
    #
    # Note that other error handling semantics is available, depending on your
    # application's needs.  The application could for example spawn
    # a replacement pilot at this point, or reduce the number of compute units
    # to match the remaining set of pilots.

    for pilot in pilots:
        print("[Callback]: Pilot '%s' state: %s." % (pilot.uid, pilot.state))

        if pilot.state == rp.FAILED:
            print(pilot.log[-1])  # Get the last log message

    return True


# -----------------------------------------------------------------------------
#
def unit_state_cb(units):
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

    print('unit cb for %d units [%s]' % (len(units), set([u.state for u in units])))
  # for unit in units:
  #     print('  unit %s: %s' % (unit.uid, unit.state))
  #
  #     if unit.state == rp.FAILED:
  #         print('                  : %s' % unit.stderr)

    return True


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # This example shows how simple error handling can be implemented
    # asynchronously using callbacks (see above)

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()
    print("session id: %s" % session.uid)

    # all other RP code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # create the managers
        umgr = rp.UnitManager(session=session)
        pmgr = rp.PilotManager(session=session)

        # Register our callbacks with the managers. The callbacks will get
        # called every time any of the pilots or units change their state
        # -- in particular also on FAILED states.
        umgr.register_callback(unit_state_cb)
        pmgr.register_callback(pilot_state_cb)

        # Create a local pilot.
        pd = rp.ComputePilotDescription()
        pd.resource  = "local.localhost"
        pd.cores     = 64
        pd.runtime   = 60

        pilot = pmgr.submit_pilots(pd)
        umgr.add_pilots(pilot)


        # we submit n tasks, some of which will fail
        n    = 10
        cuds = list()
        for _ in range(n):
            cud = rp.ComputeUnitDescription()
            if random.random() < 0.5:
                cud.executable = '/bin/true'
            else:
                cud.executable = '/bin/fail'
            cuds.append(cud)

        # submit the units...
        cus = umgr.submit_units(cuds)

        # ...and wait for their completion
        state = umgr.wait_units(state=rp.FINAL)


    except Exception as e:
        # Something unexpected happened in the pilot code above
        print("caught Exception: %s" % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print("need to exit now: %s" % e)

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print("closing session")
        session.close()


# ------------------------------------------------------------------------------

