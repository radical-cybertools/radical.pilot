"""Pilot Manager tests
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
class Test_PilotManager(unittest.TestCase):
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
    def test__pilotmanager_create(self):
        """ Test if pilot manager creation works as expected.
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        assert session.list_pilot_managers() == [], "Wrong number of pilot managers"

        pm = radical.pilot.PilotManager(session=session)
        assert session.list_pilot_managers() == [pm.uid], "Wrong list of pilot managers"

        pm = radical.pilot.PilotManager(session=session)
        assert len(session.list_pilot_managers()) == 2, "Wrong number of pilot managers"

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_reconnect(self):
        """ Test if pilot manager re-connect works as expected.
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = radical.pilot.PilotManager(session=session)
        assert session.list_pilot_managers() == [pm.uid], "Wrong list of pilot managers"

        pm_r = session.get_pilot_managers(pilot_manager_ids=pm.uid)

        assert session.list_pilot_managers() == [pm_r.uid], "Wrong list of pilot managers"

        assert pm.uid == pm_r.uid, "Pilot Manager IDs not matching!"

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_list_pilots(self):
        """ Test if listing pilots works as expected.
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm1 = radical.pilot.PilotManager(session=session)
        assert len(pm1.list_pilots()) == 0, "Wrong number of pilots returned."

        pm2 = radical.pilot.PilotManager(session=session)
        assert len(pm2.list_pilots()) == 0, "Wrong number of pilots returned."

        for i in range(0, 2):
            cpd = radical.pilot.ComputePilotDescription()
            cpd.resource = "local.localhost"
            cpd.cores = 1
            cpd.runtime = 1
            cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
            cpd.cleanup = True

            pm1.submit_pilots(pilot_descriptions=cpd)
            pm2.submit_pilots(pilot_descriptions=cpd)

        assert len(pm1.list_pilots()) == 2, "Wrong number of pilots returned."
        assert len(pm2.list_pilots()) == 2, "Wrong number of pilots returned."

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_list_pilots_after_reconnect(self):
        """ Test if listing pilots after a reconnect works as expected.
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm1 = radical.pilot.PilotManager(session=session)
        assert len(pm1.list_pilots()) == 0, "Wrong number of pilots returned."

        pm2 = radical.pilot.PilotManager(session=session)
        assert len(pm2.list_pilots()) == 0, "Wrong number of pilots returned."

        for i in range(0, 2):
            cpd = radical.pilot.ComputePilotDescription()
            cpd.resource = "local.localhost"
            cpd.cores = 1
            cpd.runtime = 1
            cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
            cpd.cleanup = True

            pm1.submit_pilots(pilot_descriptions=cpd)
            pm2.submit_pilots(pilot_descriptions=cpd)

        assert len(pm1.list_pilots()) == 2, "Wrong number of pilots returned."
        assert len(pm2.list_pilots()) == 2, "Wrong number of pilots returned."

        pm1_r = session.get_pilot_managers(pilot_manager_ids=pm1.uid)
        pm2_r = session.get_pilot_managers(pilot_manager_ids=pm2.uid)

        assert len(pm1_r.list_pilots()) == 2, "Wrong number of pilots returned."
        assert len(pm2_r.list_pilots()) == 2, "Wrong number of pilots returned."

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_get_pilots(self):
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm1 = radical.pilot.PilotManager(session=session)
        assert len(pm1.list_pilots()) == 0, "Wrong number of pilots returned."

        pm2 = radical.pilot.PilotManager(session=session)
        assert len(pm2.list_pilots()) == 0, "Wrong number of pilots returned."

        pm1_pilot_uids = []
        pm2_pilot_uids = []

        for i in range(0, 2):
            cpd = radical.pilot.ComputePilotDescription()
            cpd.resource = "local.localhost"
            cpd.cores = 1
            cpd.runtime = 1
            cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
            cpd.cleanup = True

            pilot_pm1 = pm1.submit_pilots(pilot_descriptions=cpd)
            pm1_pilot_uids.append(pilot_pm1.uid)

            pilot_pm2 = pm2.submit_pilots(pilot_descriptions=cpd)
            pm2_pilot_uids.append(pilot_pm2.uid)

        for i in pm1.list_pilots():
            pilot = pm1.get_pilots(i)
            assert pilot.uid in pm1_pilot_uids, "Wrong pilot ID %s (not in %s)" % (pilot.uid, pm1_pilot_uids)


        assert len(pm1.get_pilots()) == 2, "Wrong number of pilots."

        for i in pm2.list_pilots():
            pilot = pm2.get_pilots(i)
            assert pilot.uid in pm2_pilot_uids, "Wrong pilot ID %s" % pilot.uid

        assert len(pm2.get_pilots()) == 2, "Wrong number of pilots."

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_wait(self):
        """Test if wait() waits until all (2) pilots have reached 'DONE' state.
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pmgr = radical.pilot.PilotManager(session=session)
        
        cpd1 = radical.pilot.ComputePilotDescription()
        cpd1.resource = "local.localhost"
        cpd1.cores = 1
        cpd1.runtime = 1
        cpd1.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd1.cleanup = True

        cpd2 = radical.pilot.ComputePilotDescription()
        cpd2.resource = "local.localhost"
        cpd2.cores = 1
        cpd2.runtime = 2
        cpd2.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd2.cleanup = True

        pilots = pmgr.submit_pilots([cpd1, cpd2])

        pmgr.wait_pilots(timeout=10*60)
        
        for pilot in pilots:
            assert pilot.state == radical.pilot.DONE, "Expected state 'Done' but state is %s" % pilot.state
            assert pilot.stop_time is not None
            assert pilot.start_time is not None

        session.close()
