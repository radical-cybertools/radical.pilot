""" (Compute) Unit tests
"""

import sagapilot
import unittest

import uuid
from copy import deepcopy
from sagapilot.db import Session
from pymongo import MongoClient

DBURL = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
DBNAME = 'sagapilot_unittests'


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
        session = sagapilot.Session(database_url=DBURL)

        pm = sagapilot.PilotManager(session=session)

        cpd = sagapilot.ComputePilotDescription()
        cpd.resource = "localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/sagapilot.sandbox.unittests"

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        assert pilot is not None
        assert pilot.start_time is None
        assert pilot.stop_time is None

        pilot.wait(sagapilot.states.RUNNING)
        assert pilot.submission_time is not None
        assert pilot.state == sagapilot.states.RUNNING
        assert pilot.start_time is not None
        assert pilot.log is not None
        assert pilot.sandbox == "file://localhost%s/pilot-%s/" % (cpd.sandbox, pilot.uid)

        # the pilot should finish after it has reached run_time

        pilot.wait(sagapilot.states.DONE)
        assert pilot.state == sagapilot.states.DONE
        assert pilot.stop_time is not None

    #-------------------------------------------------------------------------
    #
    def test__pilot_cancel(self):
        """ Test if we can cancel a pilot.
        """
        session = sagapilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = sagapilot.PilotManager(session=session)

        cpd = sagapilot.ComputePilotDescription()
        cpd.resource = "localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/sagapilot.sandbox.unittests"

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        assert pilot is not None
        assert pilot.start_time is None
        assert pilot.stop_time is None

        pilot.wait(sagapilot.states.RUNNING)
        assert pilot.submission_time is not None
        assert pilot.state == sagapilot.states.RUNNING
        assert pilot.start_time is not None

        # the pilot should finish after it has reached run_time
        pilot.cancel()

        pilot.wait(sagapilot.states.CANCELED)
        assert pilot.state == sagapilot.states.CANCELED
        assert pilot.stop_time is not None
