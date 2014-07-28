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
class TestIssue114(unittest.TestCase):
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
    def test__issue_114_part_1(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/114
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "localhost"
        cpd.cores   = 1
        cpd.runtime = 5
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        all_tasks = []

        for i in range(0,4):
            cudesc = radical.pilot.ComputeUnitDescription()
            cudesc.cores      = 1
            cudesc.executable = "/bin/sleep"
            cudesc.arguments = ['180']
            all_tasks.append(cudesc)

        cu = um.submit_units(all_tasks)
        states = um.wait_units(timeout=60)

        assert states is not None
        assert radical.pilot.states.SCHEDULING in states
        assert radical.pilot.states.EXECUTING in states

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__issue_114_part_2(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/114
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "localhost"
        cpd.cores   = 1
        cpd.runtime = 5
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        cudesc = radical.pilot.ComputeUnitDescription()
        cudesc.cores      = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments = ['80']

        cu = um.submit_units(cudesc)
        state = um.wait_units(timeout=30)

        assert state == [radical.pilot.states.EXECUTING]
        assert cu.state == radical.pilot.states.EXECUTING

        state = um.wait_units()

        assert state == [radical.pilot.states.DONE]
        assert cu.state == radical.pilot.states.DONE

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__issue_114_part_3(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/114
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "localhost"
        cpd.cores   = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        state = pm.wait_pilots(timeout=30)

        assert state == [radical.pilot.states.ACTIVE]
        assert pilot.state == radical.pilot.states.ACTIVE

        state = pm.wait_pilots()

        assert state == [radical.pilot.states.DONE]
        assert pilot.state == radical.pilot.states.DONE

        session.close()
