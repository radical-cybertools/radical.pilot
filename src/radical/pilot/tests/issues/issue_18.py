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
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)
    
DBNAME = os.getenv("RADICAL_PILOT_TEST_DBNAME")
if DBNAME is None:
    print "ERROR: RADICAL_PILOT_TEST_DBNAME (MongoDB database name) is not defined."
    sys.exit(1)


#-----------------------------------------------------------------------------
#
class TestIssue18(unittest.TestCase):
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
    def test__issue_18_part_1(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/18
        """
        import saga

        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores    = 1
        cpd.runtime  = 5
        cpd.sandbox  = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup  = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        pilot.wait(state=radical.pilot.ACTIVE, timeout=5*60)

        # Now we extract the saga job id from the logs and KILL THE PROCESS
        saga_id = None
        for log_entry in pilot.log:
            if "SAGA job submitted with job id" in log_entry:
                saga_id = log_entry.replace("SAGA job submitted with job id ", "")

        if saga_id is None:
            assert False, "Couldn't find SAGA Job ID in logs."

        # KILL THE AGENT PROCESS. 
        s = saga.job.Service("fork://localhost")
        j = s.get_job(saga_id)
        j.cancel()

        pilot.wait(timeout=60)
        assert pilot.state == radical.pilot.FAILED

        session.close()

    
