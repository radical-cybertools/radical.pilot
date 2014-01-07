""" Test resources
"""

import sinon

import os
import uuid
import getpass
import unittest

from copy import deepcopy
from sinon.db import Session
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
    def test__remote_submission(self):
        """ Test saga-pilot on real backends
        """
        test_resource = os.getenv('SINON_TEST_REMOTE_RESOURCE',   "localhost")
        test_ssh_uid  = os.getenv('SINON_TEST_REMOTE_SSH_USER_ID',  None)
        test_ssh_key  = os.getenv('SINON_TEST_REMOTE_SSH_USER_KEY', None)
        test_workdir  = os.getenv('SINON_TEST_REMOTE_WORKDIR',   "/tmp/sinon.unit-tests")
        test_cores    = os.getenv('SINON_TEST_REMOTE_CORES',     "1")
        test_num_cus  = os.getenv('SINON_TEST_REMOTE_NUM_CUS',   "2")

        session = sinon.Session(database_url=DBURL, database_name=DBNAME)
        cred = sinon.SSHCredential()
        cred.user_id  = test_ssh_uid
        cred.user_key = test_ssh_key

        session.add_credential(cred)

        pm = sinon.PilotManager(session=session, resource_configurations=RESCFG)

        cpd = sinon.ComputePilotDescription()
        cpd.resource          = test_resource
        cpd.cores             = test_cores
        cpd.run_time          = 5
        cpd.working_directory = test_workdir 

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = sinon.UnitManager(session=session, scheduler='round_robin')
        um.add_pilots(pilot)

        cudescs = []
        for _ in range(0,int(test_num_cus)):
            cudesc = sinon.ComputeUnitDescription()
            cudesc.cores = 1
            cudesc.executable = "/bin/sleep"
            cudesc.arguments = ['1']
            cudescs.append(cudesc)

                
        cu = um.submit_units(cudescs)

        um.wait_units()

        #assert cu is not None
        #assert cu.submission_time is not None
        #assert cu.start_time is None
        #assert cu.start_time is None

        #cu.wait(sinon.states.RUNNING)
        #assert cu.state == sinon.states.RUNNING
        #assert cu.start_time is not None

        #cu.wait(sinon.states.DONE)
        #assert cu.state == sinon.states.DONE
        #assert cu.stop_time is not None


        pm.cancel_pilots()





