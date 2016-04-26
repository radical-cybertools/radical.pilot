#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp
import radical.utils as ru

dh = ru.DebugHelper ()

CNT       = 0

FLOPS     = 10 * 1000 * 1000 * 1000
BYTES_IN  = 10 * 1000 * 1000  # not yet supported
BYTES_OUT = 10 * 1000 * 1000  # not yet supported
BYTES_MEM = 10 * 1000 * 1000  # not yet supported

RUNTIME   = 10
UNITS     = 10
CORES     = 12

SCHED     = rp.SCHED_DIRECT_SUBMISSION

RESOURCE  = 'local.localhost'
PROJECT   = None
QUEUE     = None
SCHEMA    = None
  
# RESOURCE  = 'home.test'
# PROJECT   = None
# QUEUE     = None
# SCHEMA    = 'ssh'

# RESOURCE  = 'epsrc.archer'
# PROJECT   = 'e290'
# QUEUE     = 'short'
# SCHEMA    = None

# RESOURCE  = 'lrz.supermuc'
# PROJECT   = 'e290'
# QUEUE     = 'short'
# SCHEMA    = None

# RESOURCE  = 'xsede.stampede'
# PROJECT   = 'TG-MCB090174' 
# QUEUE     = 'development'
# SCHEMA    = None

# RESOURCE  = 'xsede.gordon'
# PROJECT   = None
# QUEUE     = 'debug'
# SCHEMA    = None

# RESOURCE  = 'xsede.blacklight'
# PROJECT   = None
# QUEUE     = 'debug'
# SCHEMA    = 'gsissh'

# RESOURCE  = 'xsede.trestles'
# PROJECT   = 'TG-MCB090174' 
# QUEUE     = 'shared'
# SCHEMA    = None

# RESOURCE  = 'futuregrid.india'
# PROJECT   = None
# QUEUE     = None
# SCHEMA    = None
  
# RESOURCE  = 'nersc.hopper'
# PROJECT   = None
# QUEUE     = 'debug'
# SCHEMA    = 'ssh'

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

        input_sd_umgr   = {'source':'/etc/group',        'target': 'f2',                'action': rp.TRANSFER}
        input_sd_agent  = {'source':'staging:///f1',     'target': 'f1',                'action': rp.COPY}
        output_sd_agent = {'source':'f1',                'target': 'staging:///f1.bak', 'action': rp.COPY}
        output_sd_umgr  = {'source':'f2',                'target': 'f2.bak',            'action': rp.TRANSFER}


        # we create one pseudo unit which installs radical.synapse in the pilot
        # ve
        cud = rp.ComputeUnitDescription()
        cud.pre_exec    = ["pip uninstall -y radical.synapse",
                           "pip install --upgrade radical.synapse"]
        cud.executable  = "radical-synapse-version"
        cud.cores       = 1

        cu = umgr.submit_units(cud)
        umgr.wait_units(cu.uid)
        assert(cu.state == rp.DONE)

        cuds = list()
        for n in range(1,UNITS+1):
            cud = rp.ComputeUnitDescription()
            cud.executable     = "radical-synapse-sample"
            cud.arguments      = ("-m sample -f %s -s %d" % (FLOPS, n)).split()
            cud.cores          = n
            cud.input_staging  = [ input_sd_umgr,  input_sd_agent]
            cud.output_staging = [output_sd_umgr, output_sd_agent]
            cuds.append(cud)

        units = umgr.submit_units(cuds)

        umgr.wait_units()

        for cu in units:
            print "* Task %s state %s, exit code: %s, started: %s, finished: %s" \
                % (cu.uid, cu.state, cu.exit_code, cu.start_time, cu.stop_time)
            print "out:"
            print cu.stdout
            print "err:"
            print cu.stderr
            print

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

