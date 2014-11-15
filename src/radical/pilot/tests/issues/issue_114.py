""" (Compute) Unit tests
"""
import os
import sys
import radical.pilot as rp
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
        session = rp.Session(database_url=DBURL, database_name=DBNAME)

        pm = rp.PilotManager(session=session)

        cpd = rp.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores    = 1
        cpd.runtime  = 5
        cpd.sandbox  = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup  = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)
        state = pm.wait_pilots(state=[rp.ACTIVE, 
                                      rp.DONE, 
                                      rp.FAILED], 
                                      timeout=5*60)

        assert (pilot.state == rp.ACTIVE), "pilot state: %s" % pilot.state

        um = rp.UnitManager(
            session=session,
            scheduler=rp.SCHED_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        all_tasks = []

        for i in range(0,2):
            cudesc = rp.ComputeUnitDescription()
            cudesc.cores      = 1
            cudesc.executable = "/bin/sleep"
            cudesc.arguments  = ['60']
            all_tasks.append(cudesc)

        units  = um.submit_units(all_tasks)
        states = um.wait_units (state=[rp.SCHEDULING, rp.EXECUTING], 
                                timeout=2*60)

        assert rp.SCHEDULING in states, "states: %s" % states

        states = um.wait_units (state=[rp.EXECUTING, rp.DONE], 
                                timeout=1*60)

        assert rp.EXECUTING  in states, "states: %s" % states

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__issue_114_part_2(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/114
        """
        session = rp.Session(database_url=DBURL, database_name=DBNAME)

        pm  = rp.PilotManager(session=session)

        cpd = rp.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores   = 1
        cpd.runtime = 5
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = rp.UnitManager(
            session=session,
            scheduler=rp.SCHED_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        state = pm.wait_pilots(state=[rp.ACTIVE, 
                                      rp.DONE, 
                                      rp.FAILED], 
                                      timeout=5*60)

        assert (pilot.state == rp.ACTIVE), "pilot state: %s" % pilot.state

        cudesc = rp.ComputeUnitDescription()
        cudesc.cores      = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments  = ['60']

        cu    = um.submit_units(cudesc)
        state = um.wait_units(state=[rp.EXECUTING], timeout=60)

        assert state    == [rp.EXECUTING], 'state   : %s' % state
        assert cu.state ==  rp.EXECUTING , 'cu state: %s' % cu.state

        state = um.wait_units(timeout=2*60)

        assert state    == [rp.DONE], 'state   : %s' % state    
        assert cu.state ==  rp.DONE , 'cu state: %s' % cu.state 

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__issue_114_part_3(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/114
        """
        session = rp.Session(database_url=DBURL, database_name=DBNAME)

        pm = rp.PilotManager(session=session)

        cpd = rp.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores   = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = rp.UnitManager(
            session   = session,
            scheduler = rp.SCHED_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        state = pm.wait_pilots(state=[rp.ACTIVE, 
                                      rp.DONE, 
                                      rp.FAILED], 
                                      timeout=10*60)

        assert state       == [rp.ACTIVE], 'state      : %s' % state    
        assert pilot.state ==  rp.ACTIVE , 'pilot state: %s' % pilot.state 

        state = pm.wait_pilots(timeout=3*60)

        assert state       == [rp.DONE], 'state      : %s' % state        
        assert pilot.state ==  rp.DONE , 'pilot state: %s' % pilot.state  

        session.close()

