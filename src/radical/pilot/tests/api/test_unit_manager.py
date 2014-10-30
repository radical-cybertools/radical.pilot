""" Unit Manager tests
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
class TestUnitManager(unittest.TestCase):
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
    def test__unitmanager_create(self):
        """ Test if unit manager creation works as expected.
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        assert session.list_unit_managers() == [], "Wrong number of unit managers"

        um1 = radical.pilot.UnitManager(session=session, scheduler='round_robin')
        assert session.list_unit_managers() == [um1.uid], "Wrong list of unit managers"

        um2 = radical.pilot.UnitManager(session=session, scheduler='round_robin')
        assert len(session.list_unit_managers()) == 2, "Wrong number of unit managers"

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__unitmanager_reconnect(self):
        """ Test if unit manager reconnection works as expected.
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        um = radical.pilot.UnitManager(session=session, scheduler='round_robin')
        assert session.list_unit_managers() == [um.uid], "Wrong list of unit managers"

        um_r = session.get_unit_managers(unit_manager_ids=um.uid)
        assert session.list_unit_managers() == [um_r.uid], "Wrong list of unit managers"

        assert um.uid == um_r.uid, "Unit Manager IDs not matching!"

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__unitmanager_pilot_assoc(self):
        """ Test if unit manager <-> pilot association works as expected. 
        """
        session = radical.pilot.Session(database_url=DBURL, database_name=DBNAME)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests" 
        cpd.cleanup = True

        p1 = pm.submit_pilots(pilot_descriptions=cpd)

        um = radical.pilot.UnitManager(session=session, scheduler='round_robin')
        assert um.list_pilots() == [], "Wrong list of pilots"

        um.add_pilots(p1)
        assert um.list_pilots() == [p1.uid], "Wrong list of pilots"

        # adding the same pilot twice should be ignored
        um.add_pilots(p1)
        assert um.list_pilots() == [p1.uid], "Wrong list of pilots"

        um.remove_pilots(p1.uid)
        assert um.list_pilots() == [], "Wrong list of pilots"

        pilot_list = []
        for x in range(0, 2):
            cpd = radical.pilot.ComputePilotDescription()
            cpd.resource = "local.localhost"
            cpd.cores = 1
            cpd.runtime = 1
            cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests" 
            cpd.cleanup = True
            p = pm.submit_pilots(pilot_descriptions=cpd)
            um.add_pilots(p)
            pilot_list.append(p)

        pl = um.list_pilots()
        assert len(pl) == 2, "Wrong number of associated pilots"
        for l in pilot_list:
            assert l in pilot_list, "Unknown pilot in list"
            um.remove_pilots(l.uid)

        assert um.list_pilots() == [], "Wrong list of pilots"

        session.close()
