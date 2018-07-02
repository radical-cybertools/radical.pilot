#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scenes!


#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit '%s' state: %s." % (unit.uid, state)

    if state == rp.FAILED:
        sys.exit (1)


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
    session = rp.Session(uid=session_name)
    print "session id: %s" % session.uid

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)
    
        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)
    
        # Define a single-core local pilot that runs for 5 minutes and cleans up
        # after itself.
        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "local.localhost"
        pdesc.cores    = 1
        pdesc.runtime  = 5 # Minutes
        #pdesc.cleanup  = True
    
        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)
    
        # Create a Compute Unit that sorts the local password file and writes the
        # output to result.dat.
        #
        #  The exact command that is executed by the agent is:
        #    "/usr/bin/sort -o result.dat passwd"
        #
        cud = rp.ComputeUnitDescription()
        cud.executable     = "/usr/bin/sort"
        cud.arguments      = ["-o", "result.dat", "passwd"]
        cud.input_staging  = "/etc/passwd"
        cud.output_staging = "result.dat"
    
        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager(session, rp.SCHEDULER_DIRECT_SUBMISSION)
    
        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)
    
        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)
    
        # Submit the previously created ComputeUnit description to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning the ComputeUnit to the ComputePilot.
        unit = umgr.submit_units(cud)
    
        # Wait for the compute unit to reach a terminal state (DONE or FAILED).
        umgr.wait_units()
    
        print "* Task %s (executed @ %s) state: %s, exit code: %s, started: %s, " \
              "finished: %s, output file: %s" % \
              (unit.uid, unit.execution_locations, unit.state,
               unit.exit_code,  unit.start_time, unit.stop_time,
               unit.description.output_staging[0]['target'])
    
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

