""" (Compute) Unit tests
"""

import os
import sys
import radical.pilot
import unittest
import time
import uuid

from pymongo import MongoClient

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)
    
DBNAME = os.getenv("RADICAL_PILOT_TEST_DBNAME")
if DBNAME is None:
    print "ERROR: RADICAL_PILOT_TEST_DBNAME (MongoDB database name) is not defined."
    sys.exit(1)


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
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

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
        assert cu.start_time is None # MS: I dont understand this assertion

        cu.wait(state=[radical.pilot.EXECUTING, radical.pilot.FAILED], timeout=5*60)
        assert cu.state == radical.pilot.EXECUTING
        assert cu.start_time is not None

        cu.wait(timeout=5*60)
        assert cu.state == radical.pilot.DONE
        assert cu.stop_time is not None

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__unit_cancel(self):
        """ Test if we can cancel a compute unit
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 60
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        # Wait until the pilot starts
        pm.wait_pilots(state=radical.pilot.ACTIVE, timeout=120)

        cudesc = radical.pilot.ComputeUnitDescription()
        cudesc.cores = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ["30"]

        cu = um.submit_units(cudesc)

        assert cu is not None
        assert cu.submission_time is not None

        # Make sure it is running!
        cu.wait(state=radical.pilot.EXECUTING, timeout=60)
        assert cu.state == radical.pilot.EXECUTING
        assert cu.start_time is not None

        # Cancel the CU!
        cu.cancel()

        cu.wait(timeout=60)
        assert cu.state == radical.pilot.CANCELED
        assert cu.stop_time is not None

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__unit_cancel_um(self):
        """ Test if we can cancel a compute unit through the UM
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 60
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        # Wait until the pilot starts
        pm.wait_pilots(state=radical.pilot.ACTIVE, timeout=240)

        cudesc = radical.pilot.ComputeUnitDescription()
        cudesc.cores = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ["60"]

        cu = um.submit_units(cudesc)

        assert cu is not None
        assert cu.submission_time is not None

        # Make sure it is running!
        cu.wait(state=radical.pilot.EXECUTING, timeout=60)
        assert cu.state == radical.pilot.EXECUTING
        assert cu.start_time is not None

        # Cancel the CU!
        um.cancel_units(cu.uid)

        cu.wait(timeout=60)
        assert cu.state == radical.pilot.CANCELED
        assert cu.stop_time is not None

        session.close()
