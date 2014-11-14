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
class TestIssue169(unittest.TestCase):
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
    def test__issue_169_part_1(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/169
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/non-/existing/directory..."
        cpd.cleanup = True

        for i in range(0, 8):
            pilot = pm.submit_pilots(pilot_descriptions=cpd)
            pilot.wait(state=radical.pilot.FAILED, timeout=5*60)
            assert pilot.state == radical.pilot.FAILED, "State is %s instead of 'Failed'." % pilot.state

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__issue_169_part_2(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/169
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pmgr = radical.pilot.PilotManager(session=session)
        
        cpd1 = radical.pilot.ComputePilotDescription()
        cpd1.resource = "local.localhost"
        cpd1.cores    = 1
        cpd1.runtime  = 1
        cpd1.sandbox  = "/tmp/radical.pilot.sandbox.unittests"
        cpd1.cleanup  = True

        cpd2 = radical.pilot.ComputePilotDescription()
        cpd2.resource = "local.localhost"
        cpd2.cores    = 1
        cpd2.runtime  = 1
        cpd2.sandbox  = "/tmp/radical.pilot.sandbox.unittests"
        cpd2.cleanup  = True

        pilots = pmgr.submit_pilots([cpd1, cpd2])

        pmgr.wait_pilots(timeout=10*60)
        
        for pilot in pilots:
            try :
                assert pilot.state == radical.pilot.DONE, "state: %s" % pilot.state
                assert pilot.stop_time  is not None,      "time : %s" % pilot.stop_time
                assert pilot.start_time is not None,      "time : %s" % pilot.start_time
            except :
                print 'pilot: %s (%s)' % (pilot.uid, pilot.state)
                for entry in pilot.state_history :
                    print '       %s : %s' % (entry.timestamp, entry.state)
                print '     : %s' % str(pilot.log)
                raise

        session.close()

