import os
import sys
import radical.pilot as rp

""" DESCRIPTION: A Simple Workload consisting one task of sequential k-means clustering.
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
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security contexts.
        session = rp.Session()

# !!!   you may need to specify a login name below, to be used in the session.
        # Add an ssh identity to the session.
        c = rp.Context('ssh')
      # c.user_id = 'osdcXX'
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initializing Pilot Manager ..."
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

# !!!   you may want to specify a different target resource below
        # this describes the parameters and requirements for our pilot job
        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = "xsede.stampede" # NOTE: This is a "label", not a hostname
        pdesc.runtime  = 5 # minutes
        pdesc.cores    = 1
        pdesc.cleanup  = True

# !!!   you may need to specify project and queue here
#       pdesc.project  = 'TG-MCB140109'
#       pdesc.queue    = 'default'

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

# !!!   # FIXME: change to a useful data file
        input     = '/data/random_1000000points.csv'
        output    = '$PWD'
        clusters  = 42
        threshold = 0.0010 # default
        cores     = 1      # Number of cores to use for once instance.

        cudesc = rp.ComputeUnitDescription()
        cudesc.cores       = cores
        cudesc.executable  = "kmeans_omp"
        cudesc.pre_exec    = ["module load intel-cluster-runtime"]
        cudesc.arguments   = ['-i', input,      # input file
                              '-z', output,     # output directory
                              '-n', clusters,   # number of clusters to find
                              '-t', threshold,  # convergence threshold
                              '-p', cores,      # number of cores, should be the same as cudesc.cores
                              '-o'              # output timing results
                              ]

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print "Submit Compute Unit to Unit Manager ..."
        cu_set = umgr.submit_units (cudesc)

        print "Waiting for CU to complete ..."
        umgr.wait_units()
        print "All CUs completed successfully!"


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

