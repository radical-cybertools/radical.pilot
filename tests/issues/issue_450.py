#!/usr/bin/env python


import os
import sys
import datetime
import radical.pilot as rp

TEST_SIZE=10 * 1024 * 1024  # 10 MB

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    print "[Callback]: ComputePilot '%s' state changed to %s at %s." % (pilot.uid, state, datetime.datetime.now())
    if  state == rp.FAILED:
        sys.exit(1)

#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state) :
    print "[Callback]: ComputeUnit '%s' state changed to %s at %s." % (unit.uid, state, datetime.datetime.now())

    if state in [rp.FAILED] :
        print "stdout: %s" % unit.stdout
        print "stderr: %s" % unit.stderr
        sys.exit(1)

#------------------------------------------------------------------------------
#
session = rp.Session()

try:

    # prepare some input files for the compute units
    os.system('head -c %d /dev/urandom > file.dat' % TEST_SIZE)


    # Add an ssh identity to the session.
    c = rp.Context('ssh')
    session.add_context(c)

    pmgr = rp.PilotManager(session=session)
    pmgr.register_callback(pilot_state_cb)

    pdescs = list()
    for i in range (1) :
        # Define a 32-core on stampede that runs for 15 minutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directory.
        pdesc = rp.ComputePilotDescription()
        pdesc.resource  = "xsede.stampede"
        pdesc.runtime   = 30 # minutes
        pdesc.cores     = 16
        pdesc.cleanup   = True
        pdesc.project   = "TG-MCB090174"

        pdescs.append (pdesc)

    pilots = pmgr.submit_pilots(pdescs)
    umgr = rp.UnitManager(session=session, scheduler=rp.SCHED_BACKFILLING)
    umgr.register_callback(unit_state_cb)
    umgr.add_pilots(pilots)

    cuds = list()
    for unit_count in range(0, 100):
        cud = rp.ComputeUnitDescription()
        cud.executable    = "/bin/bash"
        cud.arguments     = [ "-c", 'size=`cat file.dat | wc -c`; echo Size is \$size; if [ \$size != %d ]; then exit 1; fi' % TEST_SIZE]
        cud.cores         = 16
        cud.input_staging = ['file.dat']
        cuds.append(cud)

    units = umgr.submit_units(cuds)
    umgr.wait_units()

except Exception as e:
    print "TEST FAILED"
    raise

finally:
    # Remove session from database
    session.close()

    os.system ('rm -f file.dat')

# ------------------------------------------------------------------------------

