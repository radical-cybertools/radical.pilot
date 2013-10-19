#!/usr/bin/env python
# encoding: utf-8

import os
import tempfile
import unittest

from sinon.db import Session

from pymongo import MongoClient
DBURL = 'mongodb://mongohost:27017/'

#-----------------------------------------------------------------------------
#
class Test_Database(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        client     = MongoClient(DBURL)
        db         = client.sinon
        for collection in ['new_session', 'non-existing-session', 'my_new_session']:
            collection = db[collection]
            collection.drop()

    def tearDown(self):
        # shutodwn
        pass

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    #-------------------------------------------------------------------------
    #
    def test__new_session(self):
        """ Test if Session.new() behaves as expected.
        """
        try:
            # this should fail
            s = Session.new(db_url="unknownhost", sid="new_session")
            assert False, "Prev. call should have failed."
        except Exception, ex:
            assert True

        s = Session.new(db_url=DBURL, sid="new_session")
        assert s.session_id == "new_session"
        s.delete()

    #-------------------------------------------------------------------------
    #
    def test__delete_session(self):
        """ Test if session.delete() behaves as expceted.
        """
        try:
            s = Session.new(db_url=DBURL, sid="new_session")
            s.delete()
            assert False, "Prev. call should have failed."
        except Exception, ex:
            assert True

    #-------------------------------------------------------------------------
    #
    def test__reconnect_session(self):
        """ Test if Session.reconnect() behaves as expceted.
        """
        try:
            s = Session.reconnect(db_url=DBURL, sid="non-existing-session")
            assert False, "Prev. call should have failed."
        except Exception, ex:
            assert True

        s1 = Session.new(db_url=DBURL, sid="my_new_session")
        s2 = Session.reconnect(db_url=DBURL, sid="my_new_session")
        #assert s1, s2

