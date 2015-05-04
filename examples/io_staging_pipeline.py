#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import radical.pilot as rp

INPUT_FILE = 'input_file.txt'
INTERMEDIATE_FILE = 'intermediate_file.txt'
OUTPUT_FILE = 'output_file.txt'

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):
    """ this callback is invoked on all pilot state changes """

    if not pilot:
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):
    """ this callback is invoked on all unit state changes """

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if not unit:
        return

    if state in [rp.FAILED, rp.DONE, rp.CANCELED]:

        print "* unit %s (%s) state %s (%s) %s - %s, out/err: %s / %s" \
                 % (unit.uid, 
                    unit.execution_locations, 
                    unit.state, 
                    unit.exit_code, 
                    unit.start_time, 
                    unit.stop_time, 
                    unit.stdout,
                    unit.stderr)


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

        # Create input file
        radical_cockpit_occupants = ['Carol', 'Eve', 'Alice', 'Bob']
        for occ in radical_cockpit_occupants:
            os.system('/bin/echo "%s" >> %s' % (occ, INPUT_FILE))

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session)
        pmgr.register_callback(pilot_state_cb)

        # Define a C-core on stamped that runs for M minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "local.localhost"
        pdesc.runtime = 15 # M minutes
        pdesc.cores = 2 # C cores

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager(
            session=session,
            scheduler=rp.SCHED_DIRECT_SUBMISSION)
        umgr.register_callback(unit_state_cb, rp.UNIT_STATE)

        # Add the previously created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Configure the staging directive for intermediate data
        sd_inter_out = {
            'source': INTERMEDIATE_FILE,
            # Note the triple slash, because of URL peculiarities
            'target': 'staging:///%s' % INTERMEDIATE_FILE,
            'action': rp.COPY
        }

        # Task 1: Sort the input file and output to intermediate file
        cud1 = rp.ComputeUnitDescription()
        cud1.executable = 'sort'
        cud1.arguments = ['-o', INTERMEDIATE_FILE, INPUT_FILE]
        cud1.input_staging = INPUT_FILE
        cud1.output_staging = sd_inter_out

        # Submit the first task for execution.
        umgr.submit_units(cud1)

        # Wait for the compute unit to finish.
        umgr.wait_units()

        # Configure the staging directive for input intermediate data
        sd_inter_in = {
            # Note the triple slash, because of URL peculiarities
            'source': 'staging:///%s' % INTERMEDIATE_FILE,
            'target': INTERMEDIATE_FILE,
            'action': rp.LINK
        }

        # Task 2: Take the first line of the sort intermediate file and write to output
        cud2 = rp.ComputeUnitDescription()
        cud2.executable = '/bin/bash'
        cud2.arguments = ['-c', 'head -n1 %s > %s' %
                          (INTERMEDIATE_FILE, OUTPUT_FILE)]
        cud2.input_staging = sd_inter_in
        cud2.output_staging = OUTPUT_FILE

        # Submit the second CU for execution.
        umgr.submit_units(cud2)

        # Wait for the compute unit to finish.
        umgr.wait_units()

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

