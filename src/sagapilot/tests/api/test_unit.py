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
class TestUnit(unittest.TestCase):
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
    def test__unit_wait(self):
        """ Test if we can wait for different unit states.
        """
        session = sagapilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = sagapilot.PilotManager(session=session)

        cpd = sagapilot.ComputePilotDescription()
        cpd.resource = "localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/sagapilot.sandbox.unittests"

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = sagapilot.UnitManager(
            session=session,
            scheduler=sagapilot.SCHED_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        cudesc = sagapilot.ComputeUnitDescription()
        cudesc.cores = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ['10']

        cu = um.submit_units(cudesc)

        assert cu is not None
        assert cu.submission_time is not None
        assert cu.start_time is None
        assert cu.start_time is None

        cu.wait(sagapilot.states.RUNNING)
        assert cu.state == sagapilot.states.RUNNING
        assert cu.start_time is not None

        cu.wait(sagapilot.states.DONE)
        assert cu.state == sagapilot.states.DONE
        assert cu.stop_time is not None

        pm.cancel_pilots()








