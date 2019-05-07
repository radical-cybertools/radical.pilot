#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import radical.pilot as rp
import radical.utils as ru

dh = ru.DebugHelper ()

CNT      =     0
RUNTIME  =    10
SLEEP    =     1
CORES    =    16
UNITS    =    16
SCHED    = rp.SCHEDULER_ROUND_ROBIN


# ------------------------------------------------------------------------------
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


# ------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    if not unit:
        return

    global CNT

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if state in [rp.FAILED, rp.DONE, rp.CANCELED]:
        CNT += 1
        print "[Callback]: # %6d" % CNT


    if state == rp.FAILED:
        print "\nSTDOUT: %s\n\n" % unit.stdout
        print "\nSTDERR: %s\n\n" % unit.stderr
        sys.exit(2)


# ------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):

    print "[Callback]: wait_queue_size: %s." % wait_queue_size


# ------------------------------------------------------------------------------
#
def run_test(cfg):

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()
    print "session id: %s" % session.uid

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        pmgr = rp.PilotManager(session=session)
        pmgr.register_callback(pilot_state_cb)

        pdesc = rp.ComputePilotDescription()
        pdesc.resource      = cfg['cp_resource']
        pdesc.cores         = cfg['cp_cores']
        pdesc.project       = cfg['cp_project']
        pdesc.queue         = cfg['cp_queue']
        pdesc.runtime       = cfg['cp_runtime']
        pdesc.cleanup       = False
        pdesc.access_schema = cfg['cp_schema']

        pilot = pmgr.submit_pilots(pdesc)

        input_sd_pilot = {
                'source': 'file:///etc/passwd',
                'target': 'staging:///f1',
                'action': rp.TRANSFER
                }
        pilot.stage_in (input_sd_pilot)

        umgr = rp.UnitManager(session=session, scheduler=SCHED)
        umgr.register_callback(unit_state_cb,      rp.UNIT_STATE)
        umgr.register_callback(wait_queue_size_cb, rp.WAIT_QUEUE_SIZE)
        umgr.add_pilots(pilot)

        input_sd_umgr   = {'source':'file:///etc/group', 'target': 'f2',                'action': rp.COPY}
        input_sd_agent  = {'source':'staging:///f1',     'target': 'f1',                'action': rp.COPY}
        output_sd_agent = {'source':'f1',                'target': 'staging:///f1.bak', 'action': rp.COPY}
        output_sd_umgr  = {'source':'f2',                'target': 'f2.bak',            'action': rp.TRANSFER}

        cuds = list()
        for unit_count in range(0, UNITS):
            cud = rp.ComputeUnitDescription()
            cud.executable     = "wc"
            cud.arguments      = ["f1", "f2"]
            cud.cores          = 1
            cud.input_staging  = [ input_sd_umgr,  input_sd_agent]
            cud.output_staging = [output_sd_umgr, output_sd_agent]
            cuds.append(cud)

        units = umgr.submit_units(cuds)

        umgr.wait_units()

        for cu in units:
            print "* Task %s state %s, exit code: %s, started: %s, finished: %s" \
                % (cu.uid, cu.state, cu.exit_code, cu.start_time, cu.stop_time)

      # os.system ("radicalpilot-stats -m stat,plot -s %s > %s.stat" % (session.uid, session_name))


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


# -------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # TODO: the json config should be converted into an mpi_test kernel, once
    # the application kernels become maintainable...

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

# -------------------------------------------------------------------------------

