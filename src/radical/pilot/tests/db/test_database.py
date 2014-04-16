"""Database conncetion layer tests
"""

import unittest

import os
import sys
import uuid
from copy import deepcopy
from radical.pilot.db import Session
from pymongo import MongoClient

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICALPILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICALPILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)
    
DBNAME = 'radical.pilot_unittests'

#-----------------------------------------------------------------------------
#
class Test_Database():
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
    def test__new_session(self):
        """ Test if Session.new() behaves as expected.
        """
        sid = str(uuid.uuid4())
        try:
            # this should fail
            s = Session.new(sid=sid, db_url="unknownhost", db_name=DBNAME)
            assert False, "Prev. call should have failed."
        except Exception:
            assert True

        s = Session.new(sid=sid, db_url=DBURL, )
        assert s.session_id == sid
        s.delete()

    #-------------------------------------------------------------------------
    #
    def test__delete_session(self):
        """ Test if session.delete() behaves as expceted.
        """
        sid = str(uuid.uuid4())

        try:
            s = Session.new(sid=sid, db_url=DBURL, db_name=DBNAME)
            s.delete()
            assert False, "Prev. call should have failed."
        except Exception:
            assert True

    #-------------------------------------------------------------------------
    #
    def test__reconnect_session(self):
        """ Test if Session.reconnect() behaves as expceted.
        """
        sid = str(uuid.uuid4())

        try:
            s = Session.reconnect(sid=sid, db_url=DBURL, db_name=DBNAME, )
            assert False, "Prev. call should have failed."
        except Exception, ex:
            assert True

        s1 = Session.new(sid=sid, db_url=DBURL, db_name=DBNAME)
        s2 = Session.reconnect(sid=sid, db_url=DBURL, db_name=DBNAME)
        assert s1.session_id == s2.session_id
        s2.delete()

    #-------------------------------------------------------------------------
    #
    def test__insert_pilots_single(self):
        """ Tests if insert_pilots() behaves as epxected for a single pilot.
        """
        sid = str(uuid.uuid4())

        s = Session.new(sid=sid, db_url=DBURL, db_name=DBNAME)

        pilot = {
            "X": "foo",
            "Y": "bar"
        }
        ids = s.insert_pilots([pilot])
        assert len(ids) == 1, "Wrong number of IDs returned."

        pilots = s.get_raw_pilots()
        assert len(pilots) == 1, "Wrong number of pilots"
        assert pilots[0]["description"]["X"] == "foo", "Missing / wrong key."
        assert pilots[0]["state"] == "UNKNOWN", "Missing / wrong key."
        assert pilots[0]["wu_queue"] == [], "Missing / wrong key."

    #-------------------------------------------------------------------------
    #
    def test__insert_pilots_multi(self):
        """ Tests if insert_pilots() behaves as epxected for multiple pilot.
        """
        sid = str(uuid.uuid4())

        s = Session.new(sid=sid, db_url=DBURL, db_name=DBNAME)

        pilot_docs = []
        for i in range(0,32):
            pilot = {
                "X": "foo",
                "Y": str(i)
            }
            pilot_docs.append(pilot)

        ids = s.insert_pilots(pilot_docs)
        assert len(ids) == 32, "Wrong number of IDs returned."

        pilots = s.get_raw_pilots()
        for i in range(0,32):
            assert len(pilots) == 32, "Wrong number of pilots"
            assert pilots[i]["description"]["X"] == "foo", "Missing / wrong key."
            assert pilots[i]["state"] == "UNKNOWN", "Missing / wrong key."
            assert pilots[i]["wu_queue"] == [], "Missing / wrong key."

    #-------------------------------------------------------------------------
    #
    def test__insert_workunits_single(self):
        """ Tests if insert_workunits() behaves as expected for a single wu.
        """
        sid = str(uuid.uuid4())

        s = Session.new(sid=sid, db_url=DBURL, db_name=DBNAME)

        pilot = {"X": "foo",
                 "Y": "bar"
        }
        p_ids = s.insert_pilots([pilot])

        wu = {
            "description"  : {"A": "foo", "B": "bar" },
            "queue_id"     : "TODO"
        }

        wu_ids = s.insert_workunits(p_ids[0], [wu])

        workunits = s.get_raw_workunits()
        assert len(workunits) == 1, "Wrong number of workunits"
        assert workunits[0]["description"]["A"] == "foo", "Missing / wrong key."
        assert workunits[0]["assignment"]["pilot"] == p_ids[0], "Missing / wrong key."
        assert workunits[0]["state"] == "UNKNOWN", "Missing / wrong key."

        # make sure that the work units have been appended to the pilot's queue
        p = s.get_raw_pilots(p_ids)
        assert len(p[0]["wu_queue"]) == 1

    #-------------------------------------------------------------------------
    #
    def test__insert_workunits_multi(self):
        """ Tests if insert_workunits() behaves as expected for multiple wus.
        """
        sid = str(uuid.uuid4())

        s = Session.new(sid=sid, db_url=DBURL, db_name=DBNAME)

        pilot = {"X": "foo",
                 "Y": "bar"
        }
        p_ids = s.insert_pilots([pilot])

        wus = []
        for i in range(0, 128):
            wu = {
                "description"  : {"A": "foo", "B": "bar" },
                "queue_id"     : "TODO"
            }
            wus.append(wu)

        wu_ids = s.insert_workunits(p_ids[0], wus)
        assert len(wu_ids) == 128, "Wrong number of workunits"

        workunits = s.get_raw_workunits()
        assert len(workunits) == 128, "Wrong number of workunits"

        assert workunits[0]["description"]["A"] == "foo", "Missing / wrong key."
        assert workunits[0]["assignment"]["pilot"] == p_ids[0], "Missing / wrong key."
        assert workunits[0]["state"] == "UNKNOWN", "Missing / wrong key."

        # make sure that the work units have been appended to the pilot's queue
        p = s.get_raw_pilots(p_ids)
        assert len(p[0]["wu_queue"]) == 128

    #-------------------------------------------------------------------------
    #
    # def test__add_remove_work_units(self):
    #     """ Test if work_units_add(), work_units_update() and 
    #         work_units_get() behave as expected.
    #     """
    #     s = Session.new(db_url=DBURL, sid="my_new_session")

    #     wus = s.work_units_get()
    #     assert len(wus) == 0, "There shouldn't be any workunits in the collection."

    #     wu = {
    #         "work_unit_id"  : "unique work unit ID",
    #         "description"   : {
    #             "x" : "y"
    #         },
    #         "assignment"    : { 
    #             "queue" : "queue id",
    #             "pilot" : "pilot id"
    #         }
    #     }

    #     inserts = []
    #     for x in range(0,128):
    #         inserts.append(deepcopy(wu))
    #     ids = s.work_units_add(inserts)
    #     assert len(ids) == 128, "Wrong number of workunits added."




