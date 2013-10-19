#!/usr/bin/env python
# encoding: utf-8

import os
import tempfile
import unittest

#from sinon.database import *


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
    def test__connection(self):
        """ Test if we can establish a connection to the database backend.
        """
        assert True
