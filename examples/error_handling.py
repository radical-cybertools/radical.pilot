#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os

os.environ['RADICAL_PILOT_BULK_CB'] = 'True'

import random

import radical.pilot as rp

# READ: The RADICAL-Pilot documentation:
#   https://radicalpilot.readthedocs.io/en/stable/
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if
# you want to see what happens behind the scenes!


# ------------------------------------------------------------------------------
#
def pilot_state_cb(pilots):
    """
    this callback is invoked on all pilot state changes
    """

    # Callbacks happen in a different thread than the main application thread,
    # i.e., they are asynchronous.  However, this means that a 'sys.exit()'
    # does not end the application but just a thread (in this case the
    # pilot_manager_controller thread).  Thus, we wrap all threads in their own
    # try/except clauses, and then translate the `sys.exit()` into a
    # 'thread.interrupt_main()' call. This raises a 'KeyboardInterrupt' in the
    # main thread which can be interpreted by your application, for example to
    # initiate a clean shutdown via `session.close()` (see code below). The
    # same `KeyboardShutdown` is also raised when you interrupt the application
    # via `^C`.
    #
    # Note that other error handling semantics are available, depending on your
    # application requirements. For example, upon a pilot failure, the
    # application could spawn a replacement pilot, or reduce the number of
    # compute units to match the remaining set of pilots.

    for pilot in pilots:
        print("[Callback]: Pilot '%s' state: %s." % (pilot.uid, pilot.state))

        if pilot.state == rp.FAILED:
            print(pilot.log[-1])  # Get the last log message


# -----------------------------------------------------------------------------
#
def unit_state_cb(units):
    """ this callback is invoked on all unit state changes """

    # The approach to compute unit state callbacks is the same as the one to
    # pilot state callbacks. Only difference is that compute unit state
    # callbacks are invoked by the unit manager on changes of compute unit
    # states.
    #
    # The example below does not really create any ComputeUnit object, we only
    # include the callback here for documentation on the approaches to error
    # handling.
    #
    # Note that other error handling semantics are available, depending on your
    # application requirements. For example, upon units failure, the
    # application could spawn replacement units, or spawn a pilot on a
    # different resource which might be better equipped to handle the unit
    # payload.

    for unit in units:
        print('  unit %s: %s' % (unit.uid, unit.state))

        if unit.state == rp.FAILED:
            print('                  : %s' % unit.stderr)


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # This example shows how simple error handling can be asynchronously
    # implemented using callbacks (see above).

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyway...
    session = rp.Session()
    print("session id: %s" % session.uid)

    # all other RP code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus
    # tear the whole RP stack down via a 'session.close()' call in the
    # 'finally' clause...
    try:

        # create the compute unit and pilot managers.
        umgr = rp.UnitManager(session=session)
        pmgr = rp.PilotManager(session=session)

        # Register our callbacks with the managers. The callbacks will get
        # called every time any of the pilots or units change their state,
        # including on FAILED states.
        umgr.register_callback(unit_state_cb)
        pmgr.register_callback(pilot_state_cb)

        # Create a local pilot.
        pd = rp.ComputePilotDescription()
        pd.resource  = "local.localhost"
        pd.cores     = 64
        pd.runtime   = 60

        pilot = pmgr.submit_pilots(pd)
        umgr.add_pilots(pilot)


        # we submit n tasks, some of which will fail.
        n    = 1024 * 3
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

        # ... and wait for their completion.
        state = umgr.wait_units(state=rp.FINAL)


    except Exception as e:
        # Something unexpected happened in the pilot code above.
        print("caught Exception: %s" % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also
        # catch SystemExit, which gets raised if the main thread exits for
        # some other reason.
        print("need to exit now: %s" % e)

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print("closing session")
        session.close()


# ------------------------------------------------------------------------------

