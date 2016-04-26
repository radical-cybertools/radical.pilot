#!/usr/bin/env python

import os
import sys
import json
import time
import pytest
import logging
import datetime
import radical.pilot as rp

logging.raiseExceptions = False

json_data=open("../pytest_config.json")
CONFIG = json.load(json_data)
json_data.close()

#-------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        print '\nSTDOUT: %s' % pilot.stdout
        print '\nSTDERR: %s' % pilot.stderr
        print '\nLOG   : %s' % pilot.log
        sys.exit (1)

    if state in [rp.DONE, rp.FAILED, rp.CANCELED]:
        for cb in pilot.callback_history:
            print cb

#-------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):
    """ this callback is invoked on all unit state changes """

    if not unit :
        return

    global cb_counter
    cb_counter += 1

    print "[Callback]: ComputeUnit  '%s: %s' (on %s) state: %s." \
        % (unit.name, unit.uid, unit.pilot_id, state)

    if state == rp.FAILED:
        print "\nSTDOUT: %s" % unit.stdout
        print "\nSTDERR: %s" % unit.stderr
        sys.exit (1)

    if state in [rp.DONE, rp.FAILED, rp.CANCELED]:
        for cb in unit.callback_history:
            print cb

#-------------------------------------------------------------------------------
#
@pytest.fixture(scope="module")
def setup_stampede_4096(request):

    session3 = rp.Session()

    print "session id stampede: {0}".format(session3.uid)

    c = rp.Context('ssh')
    c.user_id = CONFIG["xsede.stampede"]["user_id"]
    session3.add_context(c)

    try:
        pmgr3 = rp.PilotManager(session=session3)

        print "pm id stampede: {0}".format(pmgr3.uid)

        umgr3 = rp.UnitManager (session=session3,
                               scheduler=rp.SCHED_DIRECT_SUBMISSION)

        pdesc3 = rp.ComputePilotDescription()
        pdesc3.resource = "xsede.stampede"
        pdesc3.project  = CONFIG["xsede.stampede"]["project"]
        pdesc3.runtime  = 120
        pdesc3.cores    = 4096
        pdesc3.cleanup  = False

        pilot3 = pmgr3.submit_pilots(pdesc3)
        pilot3.register_callback(pilot_state_cb)

        umgr3.add_pilots(pilot3)

    except Exception as e:
        print 'test failed'
        raise

    def fin():
        print "finalizing..."
        pmgr3.cancel_pilots()       
        pmgr3.wait_pilots() 

        print 'closing session'
        session3.close()

    request.addfinalizer(fin)

    return session3, pilot3, pmgr3, umgr3, "xsede.stampede"

#-------------------------------------------------------------------------------
#
@pytest.fixture(scope="module")
def setup_stampede_683(request):

    session = rp.Session()

    print "session id stampede: {0}".format(session.uid)

    c = rp.Context('ssh')
    c.user_id = CONFIG["xsede.stampede"]["user_id"]
    session.add_context(c)

    try:
        pmgr = rp.PilotManager(session=session)

        umgr = rp.UnitManager (session=session,
                               scheduler=rp.SCHED_BACKFILLING)

        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "xsede.stampede"
        pdesc.project  = CONFIG["xsede.stampede"]["project"]
        pdesc.runtime  = 40
        pdesc.cores    = 683
        pdesc.cleanup  = False

        pilot = pmgr.submit_pilots(pdesc)
        pilot.register_callback(pilot_state_cb)

        umgr.add_pilots(pilot)

    except Exception as e:
        print 'test failed'
        raise

    def fin():
        print "finalizing..."
        pmgr.cancel_pilots()       
        pmgr.wait_pilots() 

        print 'closing session'
        session.close()

    request.addfinalizer(fin)

    return session, pilot, pmgr, umgr, "xsede.stampede"

#-------------------------------------------------------------------------------
# add tests below...
#-------------------------------------------------------------------------------
# ATTENTION: This test consumes about 20 CPU hours on stampede
#

def test_issue_104(setup_stampede_4096):

    session, pilot, pmgr, umgr, resource = setup_stampede_4096

    compute_units = []

    for unit_count in range(0, 4 * 4096):
        cu = rp.ComputeUnitDescription()
        cu.executable = "/bin/sleep"
        cu.arguments = ["60"]
        cu.cores = 1

        compute_units.append(cu)

    units = umgr.submit_units(compute_units)

    umgr.wait_units()

    for unit in umgr.get_units():
        assert (unit.state == rp.DONE)

        # Print some information about the unit.
        print "%s" % str(unit)

        # Get the stdout and stderr streams of the ComputeUnit.
        print "  STDOUT: %s" % unit.stdout
        print "  STDERR: %s" % unit.stderr

#-------------------------------------------------------------------------------
#

def test_issue_503(setup_stampede_683):

    session, pilot, pmgr, umgr, resource = setup_stampede_683

    compute_units = []
    for unit_count in range(0, 256):

        cud = rp.ComputeUnitDescription()

        cud.executable = "/bin/sleep"
        cud.arguments = ["300"]
        cud.mpi = False
        cud.cores = 8

        compute_units.append(cud)

    umgr.register_callback(unit_state_change_cb)

    units = umgr.submit_units(compute_units)

    umgr.wait_units()

    if not isinstance(units, list):
        units = [units]

    for unit in units:
        print "* Task %s - state: %s, exit code: %s, started: %s, finished: %s, stdout: %s" \
            % (unit.uid, unit.state, unit.exit_code, unit.start_time, unit.stop_time, unit.stdout)
        
        assert (unit.state == rp.DONE)
