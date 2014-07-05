""" (Compute) Unit tests
"""
import os
import sys
import radical.pilot
import unittest

import uuid
from copy import deepcopy
from radical.pilot.db import Session
from pymongo import MongoClient

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: radical.pilot_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)
    
DBNAME = 'radicalpilot_unittests'


#-----------------------------------------------------------------------------
#
class TestIssue213(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        client = MongoClient(DBURL)
        client.drop_database(DBNAME)

        self.test_resource = os.getenv('RADICAL_PILOT_TEST_REMOTE_RESOURCE',     "localhost")
        self.test_ssh_uid  = os.getenv('RADICAL_PILOT_TEST_REMOTE_SSH_USER_ID',  None)
        self.test_ssh_key  = os.getenv('RADICAL_PILOT_TEST_REMOTE_SSH_USER_KEY', None)
        self.test_workdir  = os.getenv('RADICAL_PILOT_TEST_REMOTE_WORKDIR',      "/tmp/radical.pilot.sandbox.unittests")
        self.test_cores    = os.getenv('RADICAL_PILOT_TEST_REMOTE_CORES',        "1")
        self.test_timeout  = os.getenv('RADICAL_PILOT_TEST_TIMEOUT',             "2")
        self.test_runtime  = os.getenv('RADICAL_PILOT_TEST_RUNTIME',             "1")

    def tearDown(self):
        # clean up after ourselves 
        client = MongoClient(DBURL)
        client.drop_database(DBNAME)

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    # #-------------------------------------------------------------------------
    # #
    # def test__issue_213(self):
    #     """ https://github.com/radical-cybertools/radical.pilot/issues/213
    #     """
    #     session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)
    #
    #     pmgr = radical.pilot.PilotManager(session=session)
    #
    #     cpd1 = radical.pilot.ComputePilotDescription()
    #     cpd1.resource = "localhost"
    #     cpd1.cores = 1
    #     cpd1.runtime = 1
    #     cpd1.sandbox = "/tmp/radical.pilot.sandbox.unittests"
    #     cpd1.cleanup = True
    #
    #     pilot = pmgr.submit_pilots(cpd1)
    #
    #     umgr = radical.pilot.UnitManager(session, scheduler=radical.pilot.SCHED_DIRECT_SUBMISSIO)
    #
    #     umgr.add_pilots(pilot)
    #
    #     cud = radical.pilot.ComputeUnitDescription()
    #     cud.executable = "/bin/date"
    #
    #     for i in range(1000):
    #         session._dbs.signal_pilots(pmgr.uid, pilot.uid, 'KEEPALIVE%d' % i)
    #         umgr.submit_units(cud)
    #
    #     pilot.wait()
    #
    #     assert pilot.state == radical.pilot.states.DONE
    #     assert pilot.stop_time is not None
    #     assert pilot.start_time is not None
    #
    #     session.close(delete=False)

    #-------------------------------------------------------------------------
    #
    def test__issue_213(self):
        """ Test if we can wait for different pilot states.
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)
        c = radical.pilot.Context('ssh')
        c.user_id  = self.test_ssh_uid
        c.user_key = self.test_ssh_key

        session.add_context(c)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource          = self.test_resource
        cpd.cores             = self.test_cores
        cpd.runtime           = self.test_runtime
        cpd.sandbox           = self.test_workdir

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        assert pilot is not None

        pilot.wait(radical.pilot.states.ACTIVE, timeout=self.test_timeout*60)
        assert pilot.state == radical.pilot.states.ACTIVE

        # the pilot should finish after it has reached run_time
        pilot.wait(radical.pilot.states.DONE, timeout=self.test_timeout*60)
        assert pilot.state == radical.pilot.states.DONE

        session.close()

