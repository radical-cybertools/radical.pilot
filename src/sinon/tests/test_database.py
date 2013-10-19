#!/usr/bin/env python
# encoding: utf-8

import os
import tempfile
import unittest

from sinon.db import Session

DBURL = 'mongodb://mongohost:27017/'

#-----------------------------------------------------------------------------
#
class Test_Database(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # setup
        pass

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

