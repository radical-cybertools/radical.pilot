
import os
import sys
import radical.pilot as rp

""" DESCRIPTION: Tutorial 2: Chaining Tasks.
For every task A_n a task B_n is started consecutively.
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


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security contexts.
        session = rp.Session()

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

        NUMBER_JOBS  = 10 # the total number of cus to run

        # submit A cus to pilot job
        cudesc_list_A = []
        for i in range(NUMBER_JOBS):

            # -------- BEGIN USER DEFINED CU A_n DESCRIPTION --------- #
            cudesc = rp.ComputeUnitDescription()
            cudesc.environment = {"CU_LIST": "A", "CU_NO": "%02d" % i}
            cudesc.executable  = "/bin/echo"
            cudesc.arguments   = ['"$CU_LIST CU with id $CU_NO"']
            cudesc.cores       = 1
            # -------- END USER DEFINED CU A_n DESCRIPTION --------- #

            cudesc_list_A.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print "Submit 'A' Compute Units to Unit Manager ..."
        cu_list_A = umgr.submit_units(cudesc_list_A)

        # Chaining cus i.e submit a compute unit, when compute unit from A is successfully executed.
        # A B CU reads the content of the output file of an A CU and writes it into its own
        # output file.
        cu_list_B = []

        # We create a copy of cu_list_A so that we can remove elements from it,
        # and still reference to the original index.
        cu_list_A_copy = cu_list_A[:]
        while cu_list_A:
            for cu_a in cu_list_A:
                idx = cu_list_A_copy.index(cu_a)

                cu_a.wait ()
                print "'A' Compute Unit '%s' finished. Submitting 'B' CU ..." % idx

                # -------- BEGIN USER DEFINED CU B_n DESCRIPTION --------- #
                cudesc = rp.ComputeUnitDescription()
                cudesc.environment = {'CU_LIST': 'B', 'CU_NO': "%02d" % idx}
                cudesc.executable  = '/bin/echo'
                cudesc.arguments   = ['"$CU_LIST CU with id $CU_NO"']
                cudesc.cores       = 1
                # -------- END USER DEFINED CU B_n DESCRIPTION --------- #

                # Submit CU to Pilot Job
                cu_b = umgr.submit_units(cudesc)
                cu_list_B.append(cu_b)
                cu_list_A.remove(cu_a)

        print "Waiting for 'B' Compute Units to complete ..."
        for cu_b in cu_list_B :
            cu_b.wait ()
            print "'B' Compute Unit '%s' finished with output:" % (cu_b.uid)
            print cu_b.stdout

        print "All Compute Units completed successfully!"


    except Exception as e:
        print "An error occurred: %s" % ((str(e)))
        sys.exit (-1)

    except KeyboardInterrupt :
        print "Execution was interrupted"
        sys.exit (-1)


    except Exception as e:
        print "An error occurred: %s" % ((str(e)))
        sys.exit (-1)

    except KeyboardInterrupt :
        print "Execution was interrupted"
        sys.exit (-1)

    finally :
        print "Closing session, exiting now ..."
        session.close()

#
# ------------------------------------------------------------------------------

