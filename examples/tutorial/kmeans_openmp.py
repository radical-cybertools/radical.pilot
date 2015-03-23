#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp


""" DESCRIPTION: A Simple Workload consisting one task of parallel k-means clustering.
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
    session = rp.Session(name=session_name)
    print "session id: %s" % session.uid

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

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
        pdesc.cores    = 8
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
        cores     = 8      # Number of cores to use for once instance.

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

