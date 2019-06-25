"""API layer tests
"""

import os
import sys
import unittest

from pymongo import MongoClient

import radical.pilot as rp



# -----------------------------------------------------------------------------
#
class Test_Session(unittest.TestCase):
    # silence deprecation warnings under py3

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    # -------------------------------------------------------------------------
    #
    def test__session_create(self):
        """ Tests if creating a new session works as epxected.
        """
        for _ in range(1, 4):
            session = rp.Session()

        client = MongoClient(DBURL)
        collections = client[DBNAME].collection_names()
        assert len(collections) == 4, "Wrong number of sessions in database"

        session.close()

    # -------------------------------------------------------------------------
    #
    def test__session_reconnect(self):
        """ Tests if reconnecting to an existing session works as epxected.
        """
        session_ids = []
        for _ in range(1, 4):
            session = rp.Session()
            session_ids.append(session.uid)

        for sid in session_ids:
            session_r = rp.Session()
            assert session_r.uid == sid, "Session IDs don't match"

        session.close()

