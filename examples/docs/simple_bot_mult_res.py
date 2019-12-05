#!/usr/bin/env python

__copyright__ = "Copyright 2014-2015, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp


""" DESCRIPTION: Tutorial 1: A Simple Workload consisting of a Bag-of-Tasks
                             submitted to multiple machines
"""

# READ: The RADICAL-Pilot documentation:
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if
# you want to see what happens behind the scences!


# ------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print("[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state))

    if state == rp.FAILED:
        sys.exit (1)


# ------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    if not unit:
        return

    global CNT

    print("[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state))

    if state == rp.FAILED:
        print("stderr: %s" % unit.stderr)
        sys.exit(2)


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # we can optionally pass session name to RP
    if len(sys.argv) > 1:
        session_name = sys.argv[1]
    else:
        session_name = None

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session(name=session_name)
    print("session id: %s" % session.uid)

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        #
        # Change the user name below if you are using a remote resource
        # and your username on that resource is different from the username
        # on your local machine.
        #
        c = rp.Context('ssh')
        c.user_id = "username"
      # c.user_pass = "PutYourPasswordHere"
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print("Initializing Pilot Manager ...")
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        #
        # If you want to run this example on XSEDE Gordon and Comet, you have
        # to add your allocation ID by setting the project attribute for each
        # pilot description ot it.
        #
        # A list of preconfigured resources can be found at:
        # http://radicalpilot.readthedocs.org/en/latest/ \
        #        machconf.html#preconfigured-resources
        #

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # The pilot_list will contain the description of the pilot that will be
        # submitted
        pilot_list = list()

        # Create the description of the first pilot and add it to the list
        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = "xsede.gordon"
        pdesc.runtime  = 10
        pdesc.cores    = 1
        pdesc.cleanup  = True
        pdesc.project  = ''
        pilot_list.append(pdesc)

        # Create the description of the secind pilot and add it to the list
        pdesc2 = rp.ComputePilotDescription ()
        pdesc2.resource = "xsede.comet"
        pdesc2.runtime  = 10
        pdesc2.cores    = 1
        pdesc2.cleanup  = True
        pdesc2.project  = ''
        pilot_list.append(pdesc2)

        # Continue adding pilot by creating a new descrption and appending it to
        # the list.

        # Submit the pilot list to the Pilot Manager. Actually all the pilots are
        # submitted to the Pilot Manager at once.
        print("Submitting Compute Pilots to Pilot Manager ...")
        pilots = pmgr.submit_pilots(pilot_list)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object. The scheduler that supports multi-pilot sessions
        # is Round Robin. Direct Submittion does not.
        print("Initializing Unit Manager ...")
        umgr = rp.UnitManager (session=session,
                               scheduler=rp.SCHEDULER_ROUND_ROBIN)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)

        # Add the created ComputePilot to the UnitManager.
        print("Registering Compute Pilots with Unit Manager ...")
        umgr.add_pilots(pilots)

        NUMBER_JOBS  = 64  # the total number of cus to run

        # submit CUs to pilot job
        cudesc_list = []
        for i in range(NUMBER_JOBS):

            # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
            cudesc = rp.ComputeUnitDescription()
            cudesc.environment = {'CU_NO': i}
            cudesc.executable  = "/bin/echo"
            cudesc.arguments   = ['I am CU number $CU_NO from $HOSTNAME']
            cudesc.cores       = 1
            # -------- END USER DEFINED CU DESCRIPTION --------- #

            cudesc_list.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print("Submit Compute Units to Unit Manager ...")
        cu_set = umgr.submit_units (cudesc_list)

        print("Waiting for CUs to complete ...")
        umgr.wait_units()
        print("All CUs completed successfully!")


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
        session.close ()

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


# ------------------------------------------------------------------------------

