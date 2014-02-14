""" Test resources
"""

import sinon

import os
import uuid
import getpass
import unittest

from copy import deepcopy
from sagapilot.db import Session
from pymongo import MongoClient

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
RESCFG = 'https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json'
DBNAME = 'sinon_test'

#-----------------------------------------------------------------------------
#
class TestRemoteSubmission(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        client = MongoClient(DBURL)
        client.drop_database(DBNAME)

        self.test_resource = os.getenv('SAGAPILOT_TEST_REMOTE_RESOURCE',     "localhost")
        self.test_ssh_uid  = os.getenv('SAGAPILOT_TEST_REMOTE_SSH_USER_ID',  None)
        self.test_ssh_key  = os.getenv('SAGAPILOT_TEST_REMOTE_SSH_USER_KEY', None)
        self.test_workdir  = os.getenv('SAGAPILOT_TEST_REMOTE_WORKDIR',      "/tmp/sagapilot.sandbox.unittests")
        self.test_cores    = os.getenv('SAGAPILOT_TEST_REMOTE_CORES',        "1")
        self.test_num_cus  = os.getenv('SAGAPILOT_TEST_REMOTE_NUM_CUS',      "2")
        self.test_timeout  = os.getenv('SAGAPILOT_TEST_TIMEOUT',             "5")

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

    #-------------------------------------------------------------------------
    #
    def test__remote_simple_submission(self):
        """ Test simple remote submission with one pilot.
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)
        cred = sinon.SSHCredential()
        cred.user_id  = self.test_ssh_uid
        cred.user_key = self.test_ssh_key

        session.add_credential(cred)

        pm = sinon.PilotManager(session=session, resource_configurations=RESCFG)

        cpd = sinon.ComputePilotDescription()
        cpd.resource          = self.test_resource
        cpd.cores             = self.test_cores
        cpd.runtime           = 5
        cpd.sandbox           = self.test_workdir 

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = sinon.UnitManager(session=session, scheduler='round_robin')
        um.add_pilots(pilot)

        cudescs = []
        for _ in range(0,int(self.test_num_cus)):
            cudesc = sinon.ComputeUnitDescription()
            cudesc.cores = 1
            cudesc.executable = "/bin/sleep"
            cudesc.arguments = ['10']
            cudescs.append(cudesc)

        cus = um.submit_units(cudescs)

        for cu in cus:
            assert cu is not None
            assert cu.start_time is None
            assert cu.start_time is None

        um.wait_units(state=[sinon.states.RUNNING], timeout=self.test_timeout)

        for cu in cus:
            assert cu.state == sinon.states.RUNNING
            assert cu.start_time is not None
            assert cu.submission_time is not None


        um.wait_units(state=[sinon.states.DONE, sinon.states.FAILED], timeout=self.test_timeout)

        for cu in cus:
            assert cu.state == sinon.states.DONE
            assert cu.stop_time is not None

        pm.cancel_pilots()

    #-------------------------------------------------------------------------
    #
    def test__remote_pilot_wait(self):
        """ Test if we can wait for different pilot states. 
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)
        cred = sinon.SSHCredential()
        cred.user_id  = self.test_ssh_uid
        cred.user_key = self.test_ssh_key

        session.add_credential(cred)

        pm = sinon.PilotManager(session=session, resource_configurations=RESCFG)

        cpd = sinon.ComputePilotDescription()
        cpd.resource          = self.test_resource
        cpd.cores             = self.test_cores
        cpd.runtime           = 2
        cpd.sandbox           = self.test_workdir 

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        assert pilot is not None
        #assert cu.start_time is None
        #assert cu.start_time is None

        pilot.wait(sinon.states.RUNNING, timeout=5.0)
        assert pilot.state == sinon.states.RUNNING
        assert pilot.start_time is not None
        assert pilot.submission_time is not None


        # the pilot should finish after it has reached run_time

        pilot.wait(sinon.states.DONE, timeout=5.0)
        assert pilot.state == sinon.states.DONE
        assert pilot.stop_time is not None

    #-------------------------------------------------------------------------
    #
    def test__remote_pilot_cancel(self):
        """ Test if we can cancel a pilot. 
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)
        cred = sinon.SSHCredential()
        cred.user_id  = self.test_ssh_uid
        cred.user_key = self.test_ssh_key

        session.add_credential(cred)

        pm = sinon.PilotManager(session=session, resource_configurations=RESCFG)

        cpd = sinon.ComputePilotDescription()
        cpd.resource          = self.test_resource
        cpd.cores             = self.test_cores
        cpd.runtime           = 2
        cpd.sandbox           = self.test_workdir 

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        assert pilot is not None
        #assert cu.start_time is None
        #assert cu.start_time is None

        pilot.wait(sinon.states.RUNNING, timeout=5.0)
        assert pilot.state == sinon.states.RUNNING
        assert pilot.submission_time is not None
        assert pilot.start_time is not None

        # the pilot should finish after it has reached run_time
        pilot.cancel()

        pilot.wait(sinon.states.CANCELED, timeout=5.0)
        assert pilot.state == sinon.states.CANCELED
        assert pilot.stop_time is not None

