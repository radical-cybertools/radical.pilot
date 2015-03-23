
import os
import sys
import time
import radical.pilot as rp
import radical.utils as ru

dh = ru.DebugHelper ()

CNT      =     0
RUNTIME  =    10
SLEEP    =     1
CORES    =     1
UNITS    =     1
SCHED    = rp.SCHED_DIRECT_SUBMISSION

RESOURCE = 'local.localhost'
PROJECT  = None
QUEUE    = None
SCHEMA   = None
  
# RESOURCE = 'home.test'
# PROJECT  = None
# QUEUE    = None
# SCHEMA   = 'ssh'

# RESOURCE = 'epsrc.archer'
# PROJECT  = 'e290'
# QUEUE    = 'short'
# SCHEMA   = None

# RESOURCE = 'lrz.supermuc'
# PROJECT  = 'e290'
# QUEUE    = 'short'
# SCHEMA   = None

# RESOURCE = 'xsede.stampede'
# PROJECT  = 'TG-MCB090174' 
# QUEUE    = 'development'
# SCHEMA   = None

# RESOURCE = 'xsede.gordon'
# PROJECT  = None
# QUEUE    = 'debug'
# SCHEMA   = None

# RESOURCE = 'xsede.blacklight'
# PROJECT  = None
# QUEUE    = 'debug'
# SCHEMA   = 'gsissh'

# RESOURCE = 'xsede.trestles'
# PROJECT  = 'TG-MCB090174' 
# QUEUE    = 'shared'
# SCHEMA   = None

# RESOURCE = 'futuregrid.india'
# PROJECT  = None
# QUEUE    = None
# SCHEMA   = None
  
#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :

    if not pilot:
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state) :

    if not unit:
        return

    global CNT

    print "[Callback]: unit %s on %s : %s." % (unit.uid, unit.pilot_id, state)

    if state in [rp.FAILED, rp.DONE, rp.CANCELED] :
        CNT += 1
        print "[Callback]: # %6d" % CNT


    if  state == rp.FAILED :
        print "stderr: %s" % unit.stderr
        # do not exit


#------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):

    print "[Callback]: wait_queue_size : %s." % wait_queue_size


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    session      = None
    session_name = None

    if len(sys.argv) > 1 :
        session_name = sys.argv[1]

    try :

        session = rp.Session(name=session_name)
        sid     = session.uid
        print "session id: %s (%s)" % (sid, session_name)

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

        session.close (cleanup=False, delete=False, terminate=True)
        session = None

      # os.system ("radicalpilot-stats -m stat,plot -s %s > %s.stat" % (sid, session_name))

    except Exception as e :
        print "exception: %s" % e
        raise

    finally :
        if  session :
            session.close (cleanup=False, delete=False, terminate=True)

# ------------------------------------------------------------------------------

