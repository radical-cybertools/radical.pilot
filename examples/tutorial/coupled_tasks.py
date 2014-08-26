
import os
import sys
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
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
def main():
    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security contexts.
        session = rp.Session()

        # Add an ssh identity to the session.
        c = rp.Context('ssh')
        #c.user_id = 'osdcXX'
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initializing Pilot Manager ..."
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # this describes the parameters and requirements for our pilot job
        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = "localhost" # NOTE: This is a "label", not a hostname
        pdesc.runtime  = 5
        pdesc.cores    = 1
        pdesc.cleanup  = True

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        print "Initializing Unit Manager ..."
        umgr = rp.UnitManager(
            session=session,
            scheduler=rp.SCHED_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)

        # Add the previsouly created ComputePilot to the UnitManager.
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

        session.close()
        print "Closed session, exiting now ..."

    except Exception as e:
            print "AN ERROR OCCURRED: %s" % ((str(e)))
            return(-1)


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    sys.exit(main())

#
#------------------------------------------------------------------------------
