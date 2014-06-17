import os
import sys
import radical.pilot

""" DESCRIPTION: Tutorial 1: A Simple Workload 
Note: User must edit USER VARIABLES section
This example will not run if these values are not set.
"""

# ---------------- BEGIN REQUIRED PILOT SETUP -----------------

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to http://docs.mongodb.org.
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

# resource information
# Note: Set fields to "None" if not applicable
HOSTNAME     = "india.futuregrid.org" # remote resource
USERNAME     = "merzky"               # username on the remote resource
QUEUE        =  None # add queue you want to use
PROJECT      =  None # add project / allocation / account to charge
WALLTIME     =    10 # add pilot wallsime in minutes
PILOT_SIZE   =     1 # number of cores required for the Pilot-Job
NUMBER_JOBS  =    10 # the total number of cus to run

# Continue to USER DEFINED CU DESCRIPTION to add 
# the required information about the individual cus.

#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """

    if state == radical.pilot.states.FAILED:
        print "[Callback]: Pilot '%s' state changed to %s." % (pilot.uid, state)
        print "            Log: \n%s" % pilot.log
        sys.exit(1)


#------------------------------------------------------------------------------
#
def unit_state_change_cb(unit, state):
    """unit_state_change_cb() is a callback function. It gets called very
    time a ComputeUnit changes its state.
    """
    if state == radical.pilot.states.FAILED:
        print "[Callback]: CU '%s' state changed to '%s'." % (unit.uid, state)
        print "            Log: \n%s" % unit.log
        sys.exit(1)


#------------------------------------------------------------------------------
#
def main():

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
        # well as security crendetials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add an ssh identity to the session.
        cred = radical.pilot.SSHCredential()
        cred.user_id = USERNAME
        session.add_credential(cred)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "create pilot manager"
        pmgr = radical.pilot.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # this describes the parameters and requirements for our pilot job
        pdesc = radical.pilot.ComputePilotDescription ()
        pdesc.resource = HOSTNAME
        pdesc.runtime  = WALLTIME
        pdesc.queue    = QUEUE
        pdesc.project  = PROJECT
        pdesc.cores    = PILOT_SIZE
        pdesc.cleanup  = True

        # submit the pilot.
        print "submit pilot"
        pilot = pmgr.submit_pilots(pdesc)


        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        print "create unit manager"
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_change_cb)

        # Add the previsouly created ComputePilot to the UnitManager.
        print "add    pilot"
        umgr.add_pilots(pilot)


        # submit CUs to pilot job
        cudesc_set = list ()
        for i in range(NUMBER_JOBS):

            # -------- BEGIN USER DEFINED CU 1 DESCRIPTION --------- #
            cudesc = radical.pilot.ComputeUnitDescription()
            cudesc.environment = {'CU_NO': i}
            cudesc.executable  = "/bin/echo"
            cudesc.arguments   = ['I am CU number $CU_NO']
            cudesc.cores       = 1
            # -------- END USER DEFINED CU 1 DESCRIPTION --------- #

            cudesc_set.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print "submit units"
        cu_set = umgr.submit_units (cudesc_set)

        print "Waiting for CUs to finish..."
        for cu in cu_set :
            cu.wait ()
            print "---------------"
            print "CU '%s' finished." % (cu.uid)
            print cu.stdout


    except Exception as e:
            print "AN ERROR OCCURRED: %s" % ((str(e)))
            return(-1)


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    sys.exit(main())

#
#------------------------------------------------------------------------------

