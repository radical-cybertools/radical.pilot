""" Test resources
"""


import os
import sys
import unittest

from pymongo import MongoClient

import radical.pilot as rp



# -----------------------------------------------------------------------------
#
class TestRemoteSubmission(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        self.test_resource = os.getenv('RADICAL_PILOT_TEST_REMOTE_RESOURCE',     "local.localhost")
        self.test_ssh_uid  = os.getenv('RADICAL_PILOT_TEST_REMOTE_SSH_USER_ID',  None)
        self.test_ssh_key  = os.getenv('RADICAL_PILOT_TEST_REMOTE_SSH_USER_KEY', None)
        self.test_workdir  = os.getenv('RADICAL_PILOT_TEST_REMOTE_WORKDIR',      "/tmp/rp.sandbox.unittests")
        self.test_cores    = os.getenv('RADICAL_PILOT_TEST_REMOTE_CORES',        "1")
        self.test_num_cus  = os.getenv('RADICAL_PILOT_TEST_REMOTE_NUM_CUS',      "2")
        self.test_timeout  = os.getenv('RADICAL_PILOT_TEST_TIMEOUT',             "5")

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    # -------------------------------------------------------------------------
    #
    def test__remote_simple_submission(self):
        """ Test simple remote submission with one pilot.
        """
        session = rp.Session()
        c = rp.Context('ssh')
        c.user_id  = self.test_ssh_uid
        c.user_key = self.test_ssh_key

        session.add_context(c)

        pm = rp.PilotManager(session=session)

        cpd = rp.ComputePilotDescription()
        cpd.resource = self.test_resource
        cpd.cores = self.test_cores
        cpd.runtime = 15
        cpd.sandbox = self.test_workdir

        pilot = pm.submit_pilots(descriptions=cpd)

        um = rp.UnitManager(session=session, scheduler='round_robin')
        um.add_pilots(pilot)

        cudescs = []
        for _ in range(0,int(self.test_num_cus)):
            cudesc = rp.ComputeUnitDescription()
            cudesc.cores = 1
            cudesc.executable = "/bin/sleep"
            cudesc.arguments = ['10']
            cudescs.append(cudesc)

        cus = um.submit_units(cudescs)

        for cu in cus:
            assert cu is not None
            assert cu.start_time is None
            assert cu.stop_time is None

        ret = um.wait_units(timeout=5*60)
        print "Return states from wait: %s" % ret

        for cu in cus:
            assert cu.state == rp.DONE, "state: %s" % cu.state
            assert cu.stop_time is not None

        pm.cancel_pilots()

        session.close()

    # -------------------------------------------------------------------------
    #
    def test__remote_pilot_wait(self):
        """ Test if we can wait for different pilot states.
        """
        session = rp.Session()
        c = rp.Context('ssh')
        c.user_id  = self.test_ssh_uid
        c.user_key = self.test_ssh_key

        session.add_context(c)

        pm = rp.PilotManager(session=session)

        cpd = rp.ComputePilotDescription()
        cpd.resource          = self.test_resource
        cpd.cores             = self.test_cores
        cpd.runtime           = 2
        cpd.sandbox           = self.test_workdir

        pilot = pm.submit_pilots(descriptions=cpd)

        assert pilot is not None
        #assert cu.start_time is None
        #assert cu.start_time is None

        pilot.wait(state=rp.PMGR_ACTIVE, timeout=5*60)
        assert pilot.state == rp.PMGR_ACTIVE
        assert pilot.start_time is not None
        assert pilot.submission_time is not None


        # the pilot should finish after it has reached run_time
        pilot.wait(timeout=5*60)
        assert pilot.state == rp.DONE
        assert pilot.stop_time is not None

        session.close()

    # -------------------------------------------------------------------------
    #
    def test__remote_pilot_cancel(self):
        """ Test if we can cancel a pilot.
        """
        session = rp.Session()
        c = rp.Context('ssh')
        c.user_id  = self.test_ssh_uid
        c.user_key = self.test_ssh_key

        session.add_context(c)

        pm = rp.PilotManager(session=session)

        cpd = rp.ComputePilotDescription()
        cpd.resource          = self.test_resource
        cpd.cores             = self.test_cores
        cpd.runtime           = 2
        cpd.sandbox           = self.test_workdir

        pilot = pm.submit_pilots(descriptions=cpd)

        assert pilot is not None
        #assert cu.start_time is None
        #assert cu.start_time is None

        pilot.wait(state=rp.PMGR_ACTIVE, timeout=5*60)
        assert pilot.state == rp.PMGR_ACTIVE, "Expected state 'PMGR_ACTIVE' but got %s" % pilot.state
        assert pilot.submission_time is not None
        assert pilot.start_time is not None

        # the pilot should finish after it has reached run_time
        pilot.cancel()

        pilot.wait(timeout=5*60)
        assert pilot.state == rp.CANCELED
        assert pilot.stop_time is not None

        session.close()

