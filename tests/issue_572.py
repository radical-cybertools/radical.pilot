#!/usr/bin/env python

__copyright__ = "Copyright 2015, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp
import radical.utils as ru

dh = ru.DebugHelper ()

CNT      =     0
RUNTIME  =    10
SLEEP    =     1
CORES    =    16
UNITS    =    1
SCHED    = rp.SCHED_DIRECT_SUBMISSION

RESOURCE = 'xsede.stampede'
PROJECT  = 'TG-MCB090174'
QUEUE    = 'development'
SCHEMA   = 'ssh'
  
  
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

    if state in [rp.FAILED, rp.DONE, rp.CANCELED]:
        CNT += 1
        print "[Callback]: # %6d" % CNT


    if state == rp.FAILED:
        print "stderr: %s" % unit.stderr
        sys.exit(2)


#------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):

    print "[Callback]: wait_queue_size: %s." % wait_queue_size


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

        pmgr = rp.PilotManager(session=session)
        pmgr.register_callback(pilot_state_cb)

        pdesc = rp.ComputePilotDescription()
        pdesc.resource      = RESOURCE
        pdesc.cores         = CORES
        pdesc.project       = PROJECT
        pdesc.queue         = QUEUE
        pdesc.runtime       = RUNTIME
        pdesc.cleanup       = False
        pdesc.access_schema = SCHEMA

        pilot = pmgr.submit_pilots(pdesc)

        umgr = rp.UnitManager(session=session, scheduler=SCHED)
        umgr.register_callback(unit_state_cb,      rp.UNIT_STATE)
        umgr.register_callback(wait_queue_size_cb, rp.WAIT_QUEUE_SIZE)
        umgr.add_pilots(pilot)

        cuds = list()
        for unit_count in range(0, UNITS):
            cud = rp.ComputeUnitDescription()
            cud.pre_exec = [
                'module load gromacs',
                'echo 2 | trjconv -f tmp.gro -s tmp.gro -o tmpha.gro',
                'module load -intel +intel/14.0.1.106',
                'export PYTHONPATH=/home1/03036/jp43/.local/lib/python2.7/site-packages',
                'module load python/2.7.6',
                'export PATH=/home1/03036/jp43/.local/bin:$PATH',
                'echo "Using mpirun_rsh: `which mpirun_rsh`"'
            ]
            cud.executable = "/opt/apps/intel14/mvapich2_2_0/python/2.7.6/lib/python2.7/site-packages/mpi4py/bin/python-mpi"
            cud.arguments = ["lsdm.py", "-f", "config.ini", "-c",
                "tmpha.gro", "-n" "neighbors.nn", "-w", "weight.w"]
            cud.cores = 4
            cud.mpi = True
            cud.input_staging  = [
                'issue_572_files/config.ini',
                'issue_572_files/lsdm.py',
                'issue_572_files/tmp.gro'
            ]
            cuds.append(cud)

        units = umgr.submit_units(cuds)

        umgr.wait_units()

        for cu in units:
            print "* Task %s state %s, exit code: %s, started: %s, finished: %s" \
                % (cu.uid, cu.state, cu.exit_code, cu.start_time, cu.stop_time)

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
