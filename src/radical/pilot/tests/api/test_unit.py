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
DBURL = os.getenv("RADICALPILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICALPILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)
    
DBNAME = 'radicalpilot_unittests'


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
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        cudesc = radical.pilot.ComputeUnitDescription()
        cudesc.cores = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ['10']

        cu = um.submit_units(cudesc)

        assert cu is not None
        assert cu.submission_time is not None
        assert cu.start_time is None
        assert cu.start_time is None

        cu.wait(radical.pilot.states.RUNNING)
        assert cu.state == radical.pilot.states.RUNNING
        assert cu.start_time is not None

        cu.wait(radical.pilot.states.DONE)
        assert cu.state == radical.pilot.states.DONE
        assert cu.stop_time is not None

        session.close()
