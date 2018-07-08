#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import radical.pilot as rp
import radical.utils as ru


#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        print '\nSTDOUT: %s\n\n' % pilot.stdout
        print '\nSTDERR: %s\n\n' % pilot.stderr
        print '\nLOG   : %s\n\n' % pilot.log
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    if not unit:
        return

    global CNT

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if state == rp.FAILED:
        print "\nSTDOUT: %s\n\n" % unit.stdout
        print "\nSTDERR: %s\n\n" % unit.stderr
        sys.exit(2)


# ------------------------------------------------------------------------------
#
def run_test (cfg):

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()
    print "session id: %s" % session.uid

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initializing Pilot Manager ..."
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = cfg['cp_resource']
        if cfg['cp_schema']:
            pdesc.access_schema = cfg['cp_schema']
        pdesc.project  = cfg['cp_project']
        pdesc.queue    = cfg['cp_queue']
        pdesc.runtime  = cfg['cp_runtime']
        pdesc.cores    = cfg['cp_cores']
        pdesc.cleanup  = True

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        print "Initializing Unit Manager ..."
        umgr = rp.UnitManager (session=session,
                               scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)

        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        NUMBER_JOBS  = 10 # the total number of cus to run

        # submit CUs to pilot job
        cudesc_list = []
        for i in range(NUMBER_JOBS):

            cudesc = rp.ComputeUnitDescription()
            if cfg['cu_pre_exec']:
                cudesc.pre_exec = cfg['cu_pre_exec']
            cudesc.executable    = cfg['executable']
            cudesc.arguments     = ["helloworld_mpi.py"]
            cudesc.input_staging = ["%s/../examples/helloworld_mpi.py" % cfg['pwd']]
            cudesc.cores         = cfg['cu_cores']
            cudesc.mpi           = True

            cudesc_list.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print "Submit Compute Units to Unit Manager ..."
        cu_set = umgr.submit_units (cudesc_list)

        print "Waiting for CUs to complete ..."
        umgr.wait_units()
        print "All CUs completed successfully!"

        for unit in cu_set:
            print "* Task %s - state: %s, exit code: %s, started: %s, finished: %s, stdout: %s" \
                  % (unit.uid, unit.state, unit.exit_code, unit.start_time, unit.stop_time, unit.stdout)

            assert (unit.state == rp.DONE)
            for i in range (cfg['cu_cores']):
                assert ('mpi rank %d/%d' % (i+1, cfg['cu_cores']) in unit.stdout)


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
        raise

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print "closing session"
        print "SESSION ID: %s" % session.uid
        session.close (cleanup=False)

        # the above is equivalent to
        #
        #   session.close (cleanup=False, terminate=True)
        #
        # it will thus *not* clean out the session's database record (s that is
        # used for some statistics post-mortem), but will kill all remaining
        # pilots.


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # TODO: the json config should be converted into an mpi_test kernel, once
    # the application kernels become maintainable...
    print __file__

    pwd     = os.path.dirname(__file__)
    if not pwd:
        pwd = '.'
    configs = ru.read_json_str ('%s/test.json' % pwd)
    targets = sys.argv[1:]
    failed  = 0

    if not targets:
        print "\n\n\tusage: %s <target> [target] ...\n\n"
        sys.exit (-1)


    for target in targets:

        if not target in configs:
            print 'no config found for %s' % target
            print 'known targets: %s' % ', '.join (configs.keys())
            continue
        
        cfg = configs[target]
        cfg['cp_resource'] = target
        cfg['pwd']         = pwd

        try:
            run_test (cfg)
        except Exception as e:
            import traceback
            traceback.print_exc ()  
            print "mpi test for '%s' failed: %s" % (target, e)
            failed += 1

    sys.exit(failed)

#-------------------------------------------------------------------------------

