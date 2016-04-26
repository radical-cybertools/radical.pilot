#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import os
import radical.pilot as rp


""" DESCRIPTION: Tutorial 3: Coupled Tasks
For every task A1 and B1 a C1 is started.
"""

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!


#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    if not unit:
        return

    global CNT

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if state == rp.FAILED:
        print "stderr: %s" % unit.stderr
        sys.exit(2)


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # we can optionally pass session name to RP
    if len(sys.argv) > 1:
        session_name = sys.argv[1]
    else:
        session_name = None

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session(name=session_name,database_url=os.environ.get('RADICAL_PILOT_DBURL'))
    print "session id: %s" % session.uid

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
        c = rp.Context('userpass')
        #c.user_id = "tutorial_X"
        #c.user_pass = "PutYourPasswordHere"
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initializing Pilot Manager ..."
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # 
        # If you want to run this example on your local machine, you don't have 
        # to change anything here. 
        # 
        # Change the resource below if you want to run on a remote resource. 
        # You also might have to set the 'project' to your allocation ID if 
        # your remote resource does compute time accounting. 
        #
        # A list of preconfigured resources can be found at: 
        # http://radicalpilot.readthedocs.org/en/latest/machconf.html#preconfigured-resources
        # 
        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = "local.localhost"  # NOTE: This is a "label", not a hostname
        pdesc.runtime  = 10 # minutes
        pdesc.cores    = 1
        pdesc.cleanup  = True

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        print "Initializing Unit Manager ..."
        umgr = rp.UnitManager (session=session,
                               scheduler=rp.SCHED_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)

        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        NUMBER_JOBS  = 2 # the total number of CUs to chain

        # submit A cus to pilot job
        cudesc_list_A = []
        for idx in range(NUMBER_JOBS):

            # -------- BEGIN USER DEFINED CU 1 DESCRIPTION --------- #
            cudesc = rp.ComputeUnitDescription()
            cudesc.environment = {"CU_LIST": "A", "CU_NO": "%02d" % idx}
            cudesc.executable  = "/bin/echo"
            cudesc.arguments   = ['"$CU_LIST CU with id $CU_NO"']
            cudesc.cores       = 1
            # -------- END USER DEFINED CU 1 DESCRIPTION --------- #

            cudesc_list_A.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print "Submit Compute Units 'A' to Unit Manager ..."
        cu_set_A = umgr.submit_units(cudesc_list_A)

        # submit B cus to pilot job
        cudesc_list_B = []
        for idx in range(NUMBER_JOBS):

            # -------- BEGIN USER DEFINED CU 2 DESCRIPTION --------- #
            cudesc = rp.ComputeUnitDescription()
            cudesc.environment = {"CU_LIST": "B", "CU_NO": "%02d" % idx}
            cudesc.executable  = "/bin/echo"
            cudesc.arguments   = ['"$CU_LIST CU with id $CU_NO"']
            cudesc.cores       = 1
            # -------- END USER DEFINED CU 2 DESCRIPTION --------- #

            cudesc_list_B.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print "Submit Compute Units 'B' to Unit Manager ..."
        cu_set_B = umgr.submit_units(cudesc_list_B)


        # ---------------------------------------------------------------------
        print "Waiting for 'A' and 'B' CUs to complete..."
        umgr.wait_units()
        print "Executing 'C' tasks now..."
        # ---------------------------------------------------------------------

        # submit 'C' tasks to pilot job. each 'C' task takes the output of
        # an 'A' and a 'B' task and puts them together.
        cudesc_list_C = []
        for idx in range(NUMBER_JOBS):

            # -------- BEGIN USER DEFINED CU 3 DESCRIPTION --------- #
            cudesc = rp.ComputeUnitDescription()
            cudesc.environment = {"CU_SET": "C", "CU_NO": "%02d" % idx}
            cudesc.executable  = "/bin/echo"
            cudesc.arguments   = ['"$CU_SET CU with id $CU_NO"']
            cudesc.cores       = 1
            # -------- END USER DEFINED CU 3 DESCRIPTION --------- #

            cudesc_list_C.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print "Submit Compute Units 'C' to Unit Manager ..."
        cu_set_C = umgr.submit_units(cudesc_list_C)

        # ---------------------------------------------------------------------
        print "Waiting for 'C' CUs to complete..."
        umgr.wait_units()
        print "All CUs completed successfully!"


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

