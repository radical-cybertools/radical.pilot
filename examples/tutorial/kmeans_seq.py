import os
import sys
import radical.pilot

""" DESCRIPTION: A Simple Workload consisting one task of sequential k-means clustering.
"""

# ---------------- BEGIN REQUIRED PILOT SETUP -----------------

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to http://docs.mongodb.org.
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """

    if state == radical.pilot.states.FAILED:
        print "Compute Pilot '%s' failed, exiting ..." % pilot.uid
        sys.exit(1)

    elif state == radical.pilot.states.ACTIVE:
        print "Compute Pilot '%s' became active!" % (pilot.uid)


#------------------------------------------------------------------------------
#
def unit_state_cb(unit, state):
    """unit_state_cb() is a callback function. It gets called very
    time a ComputeUnit changes its state.
    """
    if state == radical.pilot.states.FAILED:
        print "Compute Unit '%s' failed ..." % unit.uid
        sys.exit(1)

    elif state == radical.pilot.states.DONE:
        print "Compute Unit '%s' finished with output:" % (unit.uid)
        print unit.stdout

#------------------------------------------------------------------------------
#
def main():

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security contexts.
        session = radical.pilot.Session(database_url=DBURL)

        # Add an ssh identity to the session.
        c = radical.pilot.Context('ssh')
        #c.user_id = 'osdcXX'
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initializing Pilot Manager ..."
        pmgr = radical.pilot.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # this describes the parameters and requirements for our pilot job
        pdesc = radical.pilot.ComputePilotDescription ()
        pdesc.resource = "fs2.das4.science.uva.nl" # NOTE: This is a "label", not a hostname
        pdesc.runtime  = 5 # minutes
        pdesc.cores    = 1
        pdesc.cleanup  = True

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        print "Initializing Unit Manager ..."
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)

        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        #input = '/var/scratch/marksant/kmeans_data/random_5000points.csv'
        #input = '/var/scratch/marksant/kmeans_data/random_500000points.csv'
        input = '/var/scratch/marksant/kmeans_data/random_1000000points.csv'
        #output = '/var/scratch/$USER/'
        output = '$PWD'
        clusters = 42
        threshold = 0.0010 # default

        cudesc = radical.pilot.ComputeUnitDescription()
        cudesc.cores       = 1
        cudesc.executable  = "/home/marksant/software/bin/kmeans_seq"
        cudesc.arguments   = ['-i', input, # input file
                              '-z', output, # output directory
                              '-n', clusters, # number of clusters to find
                              '-t', threshold, # convergence threshold
                              '-o' # output timing results
                              ]

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print "Submit Compute Unit to Unit Manager ..."
        cu_set = umgr.submit_units (cudesc)

        print "Waiting for CU to complete ..."
        umgr.wait_units()
        print "All CUs completed successfully!"

        session.close(delete=False)
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
