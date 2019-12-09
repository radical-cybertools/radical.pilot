#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import radical.pilot as rp

verbose  = os.environ.get('RADICAL_PILOT_VERBOSE', 'REPORT')
os.environ['RADICAL_PILOT_VERBOSE'] = verbose

""" DESCRIPTION: Tutorial 3: Coupled Tasks
For every task A1 and B1 a C1 is started.
"""

# READ: The RADICAL-Pilot documentation:
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if
# you want to see what happens behind the scences!


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    RESOURCE_LABEL = None
    PILOT_CORES    = None
    NUMBER_COUPLES = None
    CU_A_CORES     = None
    CU_B_CORES     = None
    CU_C_CORES     = None
    QUEUE          = None

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:


        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print("Initializing Pilot Manager ...")
        pmgr = rp.PilotManager(session=session)


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
        pdesc.resource = RESOURCE_LABEL
        pdesc.runtime  = 30
        pdesc.cores    = PILOT_CORES
        pdesc.cleanup  = True
        pdesc.queue    = QUEUE

        # submit the pilot.
        print("Submitting Compute Pilot to Pilot Manager ...")
        pilot = pmgr.submit_pilots(pdesc)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        print("Initializing Unit Manager ...")
        umgr = rp.UnitManager (session=session)

        # Add the created ComputePilot to the UnitManager.
        print("Registering Compute Pilot with Unit Manager ...")
        umgr.add_pilots(pilot)

        # submit A cus to pilot job
        cudesc_list_A = []
        for idx in range(NUMBER_COUPLES):

            # -------- BEGIN USER DEFINED CU 1 DESCRIPTION --------- #
            cudesc = rp.ComputeUnitDescription()
            cudesc.environment = {"CU_LIST": "A", "CU_NO": "%02d" % idx}
            cudesc.executable  = "/bin/echo"
            cudesc.arguments   = ['"$CU_LIST CU with id $CU_NO"']
            cudesc.cores       = CU_A_CORES
            # -------- END USER DEFINED CU 1 DESCRIPTION --------- #

            cudesc_list_A.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print("Submit Compute Units 'A' to Unit Manager ...")
        cu_set_A = umgr.submit_units(cudesc_list_A)

        # submit B cus to pilot job
        cudesc_list_B = []
        for idx in range(NUMBER_COUPLES):

            # -------- BEGIN USER DEFINED CU 2 DESCRIPTION --------- #
            cudesc = rp.ComputeUnitDescription()
            cudesc.environment = {"CU_LIST": "B", "CU_NO": "%02d" % idx}
            cudesc.executable  = "/bin/echo"
            cudesc.arguments   = ['"$CU_LIST CU with id $CU_NO"']
            cudesc.cores       = CU_B_CORES
            # -------- END USER DEFINED CU 2 DESCRIPTION --------- #

            cudesc_list_B.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print("Submit Compute Units 'B' to Unit Manager ...")
        cu_set_B = umgr.submit_units(cudesc_list_B)


        # ---------------------------------------------------------------------
        print("Waiting for 'A' and 'B' CUs to complete...")
        umgr.wait_units()
        print("Executing 'C' tasks now...")
        # ---------------------------------------------------------------------

        # submit 'C' tasks to pilot job. each 'C' task takes the output of
        # an 'A' and a 'B' task and puts them together.
        cudesc_list_C = []
        for idx in range(NUMBER_COUPLES):

            # -------- BEGIN USER DEFINED CU 3 DESCRIPTION --------- #
            cudesc = rp.ComputeUnitDescription()
            cudesc.environment = {"CU_SET": "C", "CU_NO": "%02d" % idx}
            cudesc.executable  = "/bin/echo"
            cudesc.arguments   = ['"$CU_SET CU with id $CU_NO"']
            cudesc.cores       = CU_C_CORES
            # -------- END USER DEFINED CU 3 DESCRIPTION --------- #

            cudesc_list_C.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print("Submit Compute Units 'C' to Unit Manager ...")
        cu_set_C = umgr.submit_units(cudesc_list_C)

        # ---------------------------------------------------------------------
        print("Waiting for 'C' CUs to complete...")
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

