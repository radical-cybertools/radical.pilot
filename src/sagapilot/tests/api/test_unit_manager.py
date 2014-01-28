""" Unit Manager tests
"""

import sinon
import unittest

import uuid
from copy import deepcopy
from sagapilot.db import Session
from pymongo import MongoClient

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
DBNAME = 'sinon_test'

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
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        assert session.list_unit_managers() == [], "Wrong number of unit managers"

        um1 = sinon.UnitManager(session=session, scheduler='round_robin')
        assert session.list_unit_managers() == [um1.uid], "Wrong list of unit managers"

        um2 = sinon.UnitManager(session=session, scheduler='round_robin')
        assert len(session.list_unit_managers()) == 2, "Wrong number of unit managers"

    #-------------------------------------------------------------------------
    #
    def test__unitmanager_reconnect(self):
        """ Test if unit manager reconnection works as expected.
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        um = sinon.UnitManager(session=session, scheduler='round_robin')
        assert session.list_unit_managers() == [um.uid], "Wrong list of unit managers"

        um_r = session.get_unit_managers(unit_manager_ids=um.uid)
        assert session.list_unit_managers() == [um_r.uid], "Wrong list of unit managers"

        assert um.uid == um_r.uid, "Unit Manager IDs not matching!"

    #-------------------------------------------------------------------------
    #
    def test__unitmanager_pilot_assoc(self):
        """ Test if unit manager <-> pilot association works as expected. 
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        pm = sinon.PilotManager(session=session)

        cpd = sinon.ComputePilotDescription()
        cpd.resource          = "localhost"
        cpd.cores             = 1
        cpd.run_time          = 1
        cpd.working_directory = "/tmp/sagapilot.sandbox.unittests" 

        p1 = pm.submit_pilots(pilot_descriptions=cpd)

        um = sinon.UnitManager(session=session, scheduler='round_robin')
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
            cpd = sinon.ComputePilotDescription()
            cpd.resource          = "localhost"
            cpd.cores             = 1
            cpd.run_time          = 1
            cpd.working_directory = "/tmp/sagapilot.sandbox.unittests" 
            p = pm.submit_pilots(pilot_descriptions=cpd)
            um.add_pilots(p)
            pilot_list.append(p)

        pl = um.list_pilots()
        assert len(pl) == 2, "Wrong number of associated pilots"
        for l in pilot_list:
            assert l in pilot_list, "Unknown pilot in list"
            um.remove_pilots(l.uid)

        assert um.list_pilots() == [], "Wrong list of pilots"



