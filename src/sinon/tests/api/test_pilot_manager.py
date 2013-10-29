"""Database conncetion layer tests
"""

import sinon
import unittest

import uuid
from copy import deepcopy
from sinon.db import Session
from pymongo import MongoClient

DBURL  = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'
DBNAME = 'sinon_test'

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
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        assert session.list_pilot_managers() == [], "Wrong number of pilot managers"

        pm = sinon.PilotManager(session=session)
        assert session.list_pilot_managers() == [pm.pmid], "Wrong list of pilot managers"

        pm = sinon.PilotManager(session=session)
        assert len(session.list_pilot_managers()) == 2, "Wrong number of pilot managers"


    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_reconnect(self):
        """ Test if pilot manager re-connect works as expected.
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        pm = sinon.PilotManager(session=session)
        assert session.list_pilot_managers() == [pm.pmid], "Wrong list of pilot managers"

        pm_r = sinon.PilotManager(pilot_manager_id=pm.pmid, session=session)
        assert session.list_pilot_managers() == [pm_r.pmid], "Wrong list of pilot managers"

        assert pm.pmid == pm_r.pmid, "Pilot Manager IDs not matching!"

    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_list_pilots(self):
        """ Test if listing pilots works as expected.
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        pm1 = sinon.PilotManager(session=session)
        assert len(pm1.list_pilots()) == 0, "Wrong number of pilots returned."

        pm2 = sinon.PilotManager(session=session)
        assert len(pm2.list_pilots()) == 0, "Wrong number of pilots returned."

        for i in range(0,10):
            pm1.submit_pilot(pilot_description={})
            pm2.submit_pilot(pilot_description={})

        assert len(pm1.list_pilots()) == 10, "Wrong number of pilots returned."
        assert len(pm2.list_pilots()) == 10, "Wrong number of pilots returned."

    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_list_pilots_after_reconnect(self):
        """ Test if listing pilots after a reconnect works as expected.
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        pm1 = sinon.PilotManager(session=session)
        assert len(pm1.list_pilots()) == 0, "Wrong number of pilots returned."

        pm2 = sinon.PilotManager(session=session)
        assert len(pm2.list_pilots()) == 0, "Wrong number of pilots returned."

        for i in range(0,10):
            pm1.submit_pilot(pilot_description={"foo": "pm1"})
            pm2.submit_pilot(pilot_description={"foo": "pm2"})

        pm1_r = sinon.PilotManager(session=session, pilot_manager_id=pm1.pmid)
        pm2_r = sinon.PilotManager(session=session, pilot_manager_id=pm2.pmid)

        assert len(pm1.list_pilots()) == 10, "Wrong number of pilots returned."
        assert len(pm2.list_pilots()) == 10, "Wrong number of pilots returned."

    #-------------------------------------------------------------------------
    #
    def test__pilotmanager_get_pilots(self):
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        pm1 = sinon.PilotManager(session=session)
        assert len(pm1.list_pilots()) == 0, "Wrong number of pilots returned."

        pm2 = sinon.PilotManager(session=session)
        assert len(pm2.list_pilots()) == 0, "Wrong number of pilots returned."

        pm1_pilot_ids = []
        pm2_pilot_ids = []

        for i in range(0,10):
            pilot_pm1 = pm1.submit_pilot(pilot_description={"foo": "pm1"})
            pm1_pilot_ids.append(pilot_pm1.id)
            pilot_pm2 = pm2.submit_pilot(pilot_description={"foo": "pm2"})
            pm1_pilot_ids.append(pilot_pm2.id)

        for i in pm1.list_pilots():
            pilot_id = pm1.get_pilot(i).id
            assert pilot_id in pm1_pilot_ids, "Wrong pilot ID %s" % pilot_id




