#!/usr/bin/env python

import os
import json
import time
import pytest
import logging
import radical.pilot as rp

logging.raiseExceptions = False

PWD = os.path.dirname(__file__)
json_data = open("%s/../pytest_config.json" % PWD)
CONFIG = json.load(json_data)
json_data.close()


# ------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        print '\nSTDOUT: %s' % pilot.stdout
        print '\nSTDERR: %s' % pilot.stderr
        print '\nLOG   : %s' % pilot.log

    if state in [rp.DONE, rp.FAILED, rp.CANCELED]:
        for cb in pilot.callback_history:
            print cb


# ------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):
    """ this callback is invoked on all unit state changes """

    if not unit :
        return

    # global cb_counter
    # cb_counter += 1

    print "[Callback]: ComputeUnit  '%s: %s' (on %s) state: %s." \
        % (unit.name, unit.uid, unit.pilot_id, state)

    if state == rp.FAILED:
        print "\nSTDOUT: %s" % unit.stdout
        print "\nSTDERR: %s" % unit.stderr

    if state in [rp.DONE, rp.FAILED, rp.CANCELED]:
        for cb in unit.callback_history:
            print cb


# ------------------------------------------------------------------------------
#
@pytest.fixture(scope="module")
def setup_local_1(request):

    session1 = rp.Session()

    print "session id local_1: {0}".format(session1.uid)

    try:
        pmgr1 = rp.PilotManager(session=session1)

        print "pm id local_1: {0}".format(pmgr1.uid)

        umgr1 = rp.UnitManager (session=session1,
                               scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

        pdesc1 = rp.ComputePilotDescription()
        pdesc1.resource = "local.localhost"
        pdesc1.runtime  = 30
        pdesc1.cores    = 1
        pdesc1.cleanup  = False

        pilot1 = pmgr1.submit_pilots(pdesc1)
        pilot1.register_callback(pilot_state_cb)

        umgr1.add_pilots(pilot1)

    except Exception:
        print 'test failed'
        raise

    def fin():
        print "finalizing..."
        pmgr1.cancel_pilots()       
        pmgr1.wait_pilots() 

        print 'closing session'
        session1.close()
        time.sleep(5)

    request.addfinalizer(fin)

    return session1, pilot1, pmgr1, umgr1, "local.localhost" 


# ------------------------------------------------------------------------------
#
@pytest.fixture(scope="module")
def setup_local_2(request):

    session1 = rp.Session()

    print "session id local_2: {0}".format(session1.uid)

    try:
        pmgr1 = rp.PilotManager(session=session1)

        print "pm id local_2: {0}".format(pmgr1.uid)

        umgr1 = rp.UnitManager (session=session1,
                               scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

        pdesc1 = rp.ComputePilotDescription()
        pdesc1.resource = "local.localhost"
        pdesc1.runtime  = 30
        pdesc1.cores    = 1
        pdesc1.cleanup  = False

        pilot1 = pmgr1.submit_pilots(pdesc1)
        pilot1.register_callback(pilot_state_cb)

        umgr1.add_pilots(pilot1)

    except Exception:
        print 'test failed'
        raise

    def fin():
        print "finalizing..."
        pmgr1.cancel_pilots()       
        pmgr1.wait_pilots() 

        print 'closing session'
        session1.close()
        time.sleep(5)

    request.addfinalizer(fin)

    return session1, pilot1, pmgr1, umgr1, "local.localhost"


# ------------------------------------------------------------------------------
#
@pytest.fixture(scope="class")
def setup_gordon(request):

    session1 = rp.Session()

    print "session id gordon: {0}".format(session1.uid)


    c = rp.Context('ssh')
    c.user_id = CONFIG["xsede.gordon"]["user_id"]
    session1.add_context(c)

    try:
        pmgr1 = rp.PilotManager(session=session1)

        print "pm id gordon: {0}".format(pmgr1.uid)

        umgr1 = rp.UnitManager (session=session1,
                               scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

        pdesc1 = rp.ComputePilotDescription()
        pdesc1.resource = "xsede.gordon"
        pdesc1.project  = CONFIG["xsede.gordon"]["project"]
        pdesc1.runtime  = 30
        pdesc1.cores    = 16
        pdesc1.cleanup  = False

        pilot1 = pmgr1.submit_pilots(pdesc1)
        pilot1.register_callback(pilot_state_cb)

        umgr1.add_pilots(pilot1)

    except Exception:
        print 'test failed'
        raise

    def fin():
        print "finalizing..."
        pmgr1.cancel_pilots()       
        pmgr1.wait_pilots() 

        print 'closing session'
        session1.close()
        time.sleep(5)

    request.addfinalizer(fin)

    return session1, pilot1, pmgr1, umgr1, "xsede.gordon"


# ------------------------------------------------------------------------------
#
@pytest.fixture(scope="class")
def setup_comet(request):

    session2 = rp.Session()

    print "session id comet: {0}".format(session2.uid)

    c = rp.Context('ssh')
    c.user_id = CONFIG["xsede.comet"]["user_id"]
    session2.add_context(c)

    try:
        pmgr2 = rp.PilotManager(session=session2)

        print "pm id gordon: {0}".format(pmgr2.uid)

        umgr2 = rp.UnitManager (session=session2,
                               scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

        pdesc2 = rp.ComputePilotDescription()
        pdesc2.resource = "xsede.comet"
        pdesc2.project  = CONFIG["xsede.comet"]["project"]
        pdesc2.runtime  = 30
        pdesc2.cores    = 24
        pdesc2.cleanup  = False

        pilot2 = pmgr2.submit_pilots(pdesc2)
        pilot2.register_callback(pilot_state_cb)

        umgr2.add_pilots(pilot2)

    except Exception:
        print 'test failed'
        raise

    def fin():
        print "finalizing..."
        pmgr2.cancel_pilots()       
        pmgr2.wait_pilots() 

        print 'closing session'
        session2.close()

    request.addfinalizer(fin)

    return session2, pilot2, pmgr2, umgr2, "xsede.comet"


# ------------------------------------------------------------------------------
#
@pytest.fixture(scope="class")
def setup_stampede(request):

    session3 = rp.Session()

    print "session id stampede: {0}".format(session3.uid)

    c = rp.Context('ssh')
    c.user_id = CONFIG["xsede.stampede"]["user_id"]
    session3.add_context(c)

    try:
        pmgr3 = rp.PilotManager(session=session3)

        print "pm id stampede: {0}".format(pmgr3.uid)

        umgr3 = rp.UnitManager (session=session3,
                               scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

        pdesc3 = rp.ComputePilotDescription()
        pdesc3.resource = "xsede.stampede"
        pdesc3.project  = CONFIG["xsede.stampede"]["project"]
        pdesc3.runtime  = 60
        pdesc3.cores    = int(CONFIG["xsede.stampede"]["cores"])
        pdesc3.cleanup  = False

        pilot3 = pmgr3.submit_pilots(pdesc3)
        pilot3.register_callback(pilot_state_cb)

        umgr3.add_pilots(pilot3)

    except Exception:
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


# ------------------------------------------------------------------------------
#
@pytest.fixture(scope="class")
def setup_stampede_two(request):

    session3 = rp.Session()

    print "session id stampede: {0}".format(session3.uid)

    c = rp.Context('ssh')
    c.user_id = CONFIG["xsede.stampede"]["user_id"]
    session3.add_context(c)

    try:
        pmgr3 = rp.PilotManager(session=session3)

        print "pm id stampede: {0}".format(pmgr3.uid)

        umgr3 = rp.UnitManager (session=session3,
                               scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

        pdesc3 = rp.ComputePilotDescription()
        pdesc3.resource = "xsede.stampede"
        pdesc3.project  = CONFIG["xsede.stampede"]["project"]
        pdesc3.runtime  = 20
        pdesc3.cores    = int(CONFIG["xsede.stampede"]["cores"]) * 2
        pdesc3.cleanup  = False

        pilot3 = pmgr3.submit_pilots(pdesc3)
        pilot3.register_callback(pilot_state_cb)

        umgr3.add_pilots(pilot3)

    except Exception:
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


# ------------------------------------------------------------------------------
# add tests below...
# ------------------------------------------------------------------------------
# 
class TestRemoteOne(object):

    def test_pass_one(self, setup_stampede):

        session, pilot, pmgr, umgr, resource = setup_stampede

        umgr.register_callback(unit_state_cb)

        print "session id test: {0}".format(session.uid)

        compute_units = []
        for unit_count in range(0, 4):
            cu = rp.ComputeUnitDescription()
            cu.executable = "/bin/date"
            cu.cores = 1

            compute_units.append(cu)

        units = umgr.submit_units(compute_units)

        umgr.wait_units()

        # Wait for all compute units to finish.
        for unit in units:
            unit.wait()

        for unit in units:
            assert (unit.state == rp.DONE)

    # --------------------------------------------------------------------------
    #
    def test_pass_mpi_one(self, setup_stampede):

        session, pilot, pmgr, umgr, resource = setup_stampede

        umgr.register_callback(unit_state_cb)

        compute_units = []

        for i in range(16):
            cudesc = rp.ComputeUnitDescription()
            cudesc.pre_exec      = CONFIG[resource]["pre_exec"]
            cudesc.executable    = ["python"]
            cudesc.arguments     = ["helloworld_mpi.py"]
            cudesc.input_staging = ["../helloworld_mpi.py"]
            cudesc.cores         = int(CONFIG[resource]["cores"])
            cudesc.mpi           = True

            compute_units.append(cudesc)

        units = umgr.submit_units(compute_units)

        umgr.wait_units()

        for unit in units:
            # print "* Task %s - state: %s, exit code: %s, started: %s, finished: %s, stdout: %s" \
            #      % (unit.uid, unit.state, unit.exit_code, unit.start_time, unit.stop_time, unit.stdout)

            assert (unit.state == rp.DONE)
            for i in range(int(CONFIG[resource]["cores"])):
                assert ('mpi rank %d/%d' % (i, int(CONFIG[resource]["cores"])) in unit.stdout)

    # --------------------------------------------------------------------------
    # issue 572
    #
    def test_fail_issue_572(self, setup_stampede):

        session, pilot, pmgr, umgr, resource = setup_stampede

        umgr.register_callback(unit_state_cb)

        cuds = []
        for unit_count in range(0, 5):
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
            cud.input_staging  = ['../issue_572_files/config.ini',
                                  '../issue_572_files/lsdm.py',
                                  '../issue_572_files/tmp.gro']
            cuds.append(cud)

        units = umgr.submit_units(cuds)

        umgr.wait_units()

        for cu in units:
            cu.wait()

        for cu in units:
            assert (cu.state == rp.DONE)


# ------------------------------------------------------------------------------
#
class TestRemoteTwo(object):
    # --------------------------------------------------------------------------
    # multi-node mpi executable
    #
    def test_pass_mpi_two(self, setup_stampede_two):

        session, pilot, pmgr, umgr, resource = setup_stampede_two

        umgr.register_callback(unit_state_cb)

        compute_units = []

        proc = int(CONFIG[resource]["cores"]) * 2

        for i in range(16):
            cudesc = rp.ComputeUnitDescription()
            cudesc.pre_exec      = CONFIG[resource]["pre_exec"]
            cudesc.executable    = ["python"]
            cudesc.arguments     = ["helloworld_mpi.py"]
            cudesc.input_staging = ["../helloworld_mpi.py"]
            cudesc.cores         = proc
            cudesc.mpi           = True

            compute_units.append(cudesc)

        units = umgr.submit_units(compute_units)

        umgr.wait_units()

        for unit in units:
            assert (unit.state == rp.DONE)
            for i in range(proc):
                assert ('mpi rank %d/%d' % (i, proc) in unit.stdout)


# ------------------------------------------------------------------------------
# issue 172
#
def test_fail_issue_172(setup_stampede):

    session, pilot, pmgr, umgr, resource = setup_stampede

    umgr.register_callback(unit_state_cb)

    # generate some units which use env vars in different ways, w/ and w/o MPI
    env_variants = ['UNDEFINED',     # Special case: env will not be set
                    None,            # None
                    {},              # empty dict
                    {'foo': 'bar'},  # single entry dict
                    {'foo': 'bar', 'sports': 'bar', 'banana': 'bar'}
                                     # multi entry dict
                   ]

    compute_units = []
    idx = 1
    for env in env_variants:

        # Serial
        cud             = rp.ComputeUnitDescription()
        cud.name        = "serial_" + str(idx)
        cud.executable  = "/bin/echo"
        cud.arguments   = ['Taverns:', '$foo', '$sports', 
                           '$banana', 'dollars\$\$', '"$dollar"', 
                           'sitting \'all\' by myself', 
                           'drinking "cheap" beer']
        if env != 'UNDEFINED':
            cud.environment = env

        compute_units.append(cud)

        # MPI
        cud                 = rp.ComputeUnitDescription()
        cud.name            = "mpi_" + str(idx)
        cud.pre_exec        = CONFIG[resource]["pre_exec"]
        cud.executable      = "python"
        cud.input_staging   = ["mpi4py_env.py"]
        cud.arguments       = 'mpi4py_env.py'
        cud.cores           = 2
        cud.mpi             = True
        if  env != 'UNDEFINED' :
            cud.environment = env

        compute_units.append(cud)
        idx += 1

    units = umgr.submit_units(compute_units)

    umgr.wait_units()

    if not isinstance(units, list):
        units = [units]

    for unit in units:
        print unit.stdout
        print "\n\n"
        print "* Task %s - env: %s state: %s, exit code: %s, started: %s, finished: %s, stdout: %s" \
            % (unit.uid, unit.description.environment, unit.state,
               unit.exit_code, unit.start_time, unit.stop_time, repr(unit.stdout))

        assert (unit.state == rp.DONE)
        if unit.name == "serial_1" or unit.name == "serial_2" or unit.name == "serial_3":
            assert("Taverns:    dollars$$ \"\"" in unit.stdout)

        if unit.name == "mpi_1" or unit.name == "mpi_2" or unit.name == "mpi_3":
            assert("Taverns: None, None, None" in unit.stdout)

        if unit.name == "serial_4":
            assert("Taverns: bar   dollars$$ \"\"" in unit.stdout)

        if unit.name == "mpi_4":
            assert("Taverns: bar, None, None" in unit.stdout)

        if unit.name == "serial_5":
            assert("Taverns: bar bar bar dollars$$ \"\"" in unit.stdout)

        if unit.name == "mpi_5":
            assert("Taverns: bar, bar, bar" in unit.stdout)


# ------------------------------------------------------------------------------
# issue 359; check! does not work now...
#
def test_pass_issue_359():

    session = rp.Session()

    try:
        c = rp.Context('ssh')
        c.user_id = CONFIG["xsede.stampede"]["user_id"]
        session.add_context(c)

        pmgr = rp.PilotManager(session=session)
        pmgr.register_callback(pilot_state_cb)

        core_configs = [8, 16, 17, 32, 33]

        umgr_list = []
        for cores in core_configs:

            umgr = rp.UnitManager(session=session, scheduler=rp.SCHEDULER_DIRECT_SUBMISSION)

            umgr.register_callback(unit_state_cb)

            pdesc          = rp.ComputePilotDescription()
            pdesc.resource = "xsede.stampede"
            pdesc.project  = CONFIG["xsede.stampede"]["project"]
            pdesc.runtime  = 10
            pdesc.cores    = cores 

            pilot = pmgr.submit_pilots(pdesc)

            umgr.add_pilots(pilot)

            umgr_list.append(umgr)

        unit_list = []

        for umgr in umgr_list:

            test_task = rp.ComputeUnitDescription()

            test_task.pre_exec = CONFIG["xsede.stampede"]["pre_exec"]
            test_task.input_staging = ["../helloworld_mpi.py"]
            test_task.executable = "python"
            test_task.arguments = ["helloworld_mpi.py"]
            test_task.mpi = True
            test_task.cores = 8

            unit = umgr.submit_units(test_task)

            unit_list.append(unit)

        for umgr in umgr_list:
            umgr.wait_units()

        for unit in unit_list:
            print "* Task %s - state: %s, exit code: %s, started: %s, finished: %s, stdout: %s" \
                % (unit.uid, unit.state, unit.exit_code, unit.start_time, unit.stop_time, unit.stdout)

            assert(unit.state == rp.DONE)

    except Exception:
        print 'test failed'
        raise

    finally:
        pmgr.cancel_pilots()       
        pmgr.wait_pilots() 

        session.close()


# ------------------------------------------------------------------------------
# issue 450, fails
#
def test_fail_issue_450(setup_stampede):

    TEST_SIZE = 10 * 1024 * 1024  # 10 MB

    os.system('head -c %d /dev/urandom > file.dat' % TEST_SIZE)

    session, pilot, pmgr, umgr, resource = setup_stampede

    umgr.register_callback(unit_state_cb)

    compute_units = []
    for unit_count in range(0, 16):
        cud               = rp.ComputeUnitDescription()
        cud.executable    = "/bin/bash"
        cud.arguments     = ["-c", 'size=`cat file.dat | wc -c`; echo Size is \$size; if [ \$size != %d ]; then exit 1; fi' % TEST_SIZE]
        cud.cores         = 1
        cud.input_staging = ['file.dat']
        compute_units.append(cud)

    units = umgr.submit_units(compute_units)

    umgr.wait_units()

    for unit in units:
        unit.wait()

    for unit in units:
        assert (unit.state == rp.DONE)
        assert("Size is 10485760" in unit.stdout)


# ------------------------------------------------------------------------------
# issue 57
#
def test_pass_issue_57():

    for i in [16, 32, 64]:

        session = rp.Session()

        try:

            c = rp.Context('ssh')
            c.user_id = CONFIG["xsede.stampede"]["user_id"]
            session.add_context(c)

            pmgr = rp.PilotManager(session=session)
            umgr = rp.UnitManager(session=session, 
                                  scheduler=rp.SCHEDULER_ROUND_ROBIN) 

            pdesc = rp.ComputePilotDescription()
            pdesc.resource = "xsede.stampede"
            pdesc.project  = CONFIG["xsede.stampede"]["project"]
            pdesc.cores = i
            pdesc.runtime = 20
            pdesc.cleanup = False

            pilots = pmgr.submit_pilots(pdesc)

            umgr.add_pilots(pilots)

            unit_descrs = []
            for k in range(0, i * 2):
                cu = rp.ComputeUnitDescription()
                cu.cores = 1
                cu.executable = "/bin/date"
                unit_descrs.append(cu)

            units = umgr.submit_units(unit_descrs)

            umgr.wait_units()

            for unit in units:
                unit.wait()

            pmgr.cancel_pilots()       
            pmgr.wait_pilots()

        except Exception:
            print "TEST FAILED"
            raise

        finally:
            session.close()

