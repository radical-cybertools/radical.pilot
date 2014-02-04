""" (Compute) Unit tests
"""

import sinon
import unittest

import uuid
from copy import deepcopy
from sagapilot.db import Session
from pymongo import MongoClient

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
DBNAME = 'sinon_test'

#-----------------------------------------------------------------------------
#
class TestPilot(unittest.TestCase):
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
    def test__pilot_wait(self):
        """ Test if we can wait for different pilot states. 
        """
        session = sinon.Session(database_url=DBURL)

        pm = sinon.PilotManager(session=session)

        cpd = sinon.ComputePilotDescription()
        cpd.resource          = "localhost"
        cpd.cores             = 1
        cpd.run_time          = 1
        cpd.working_directory = "/tmp/sagapilot.sandbox.unittests" 

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        assert pilot is not None
        #assert cu.start_time is None
        #assert cu.start_time is None

        pilot.wait(sinon.states.RUNNING)
        assert pilot.submission_time is not None
        assert pilot.state == sinon.states.RUNNING
        assert pilot.start_time is not None

        # the pilot should finish after it has reached run_time

        pilot.wait(sinon.states.DONE)
        assert pilot.state == sinon.states.DONE
        assert pilot.stop_time is not None

    #-------------------------------------------------------------------------
    #
    def test__pilot_cancel(self):
        """ Test if we can cancel a pilot. 
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        pm = sinon.PilotManager(session=session)

        cpd = sinon.ComputePilotDescription()
        cpd.resource          = "localhost"
        cpd.cores             = 1
        cpd.run_time          = 1
        cpd.working_directory = "/tmp/sagapilot.sandbox.unittests" 

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        assert pilot is not None
        #assert cu.start_time is None
        #assert cu.start_time is None

        pilot.wait(sinon.states.RUNNING)
        assert pilot.submission_time is not None
        assert pilot.state == sinon.states.RUNNING
        assert pilot.start_time is not None

        # the pilot should finish after it has reached run_time
        pilot.cancel()

        pilot.wait(sinon.states.CANCELED)
        assert pilot.state == sinon.states.CANCELED
        assert pilot.stop_time is not None





