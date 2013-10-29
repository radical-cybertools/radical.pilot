""" Unit Manager tests
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
        """ Test if pilot manager creation works as expected.
        """
        session = sinon.Session(database_url=DBURL, database_name=DBNAME)

        assert session.list_unit_managers() == [], "Wrong number of unit managers"

        um1 = sinon.UnitManager(session=session)
        assert session.list_unit_managers() == [um1.umid], "Wrong list of unit managers"

        um2 = sinon.UnitManager(session=session)
        assert len(session.list_unit_managers()) == 2, "Wrong number of unit managers"