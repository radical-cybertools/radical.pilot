#!/usr/bin/env python
# encoding: utf-8

from pymongo import MongoClient

#-----------------------------------------------------------------------------
#

class Session():

    def __init__(self, db_url):
        """ Le constructeur. Should not be called directrly, but rather
            via the static methods new() or reconnect().
        """
        self._client = MongoClient(db_url)
        self._db     = self._client.sinon

        self._collection = None
        self._session_id = None

    @staticmethod
    def new(db_url, sid):
        """ Create a new session.
        """
        s = Session(db_url)
        s._create(sid)
        return s

    @staticmethod
    def reconnect(db_url, sid):
        """ Reconnect to an existing session.
        """
        s = Session(db_url)
        s._reconnect(sid)
        return s

    @property
    def session_id(self):
        """ Return the session id.
        """
        return self._session_id

    def delete(self):
        """ Remove session and all associated collections from the DB.
        """
        if self._collection is None:
            raise Exception("No active session.")
        self._collection.drop()

    def add_pilots(self, pilot_entries):
        """ Add one or more pilot entries to the session.

            A pilot entry has the following format:

            {
                "pilot_id"   : "unique string",
                "name"       : "descriptive name"
                "description : {

                },
                "info"       : {

                } 
            }
        """
        pass

    def remove_pilots(self, pilot_ids):
        """ Remove one or more pilot entries from the session.
        """
        pass

    def add_queues(self, queue_entries):
        """ Add one or more queue entries to the database.

            A queue entry has the following format:

            {
                "queue_id"  : "unique string",
                "name"      : "descriptive name",
                "scheduler" : "scheduler name"
                "pilots"    : ["pilot_id 1", "pilot_id 2", "..."]
            }

        """
        pass

    def remove_queue(self, queue_ids):
        """ Remove one or more queue entries from the database.
        """
        pass

    def _reconnect(self, sid):
        """ Reconnect to an existing session (private).
        """
        if sid not in self._db.collection_names():
            raise Exception("Session ID '%s' doesn't exists in DB." % sid)
        self._collection = self._db["%s" % sid]
        self._session_id = sid

    def _create(self, sid):
        """ Create a new session (private).
        """
        if sid in self._db.collection_names():
            raise Exception("Session ID '%s' already exists in DB." % sid)
        self._collection = self._db["%s" % sid]
        self._collection.insert({'created': True})
        self._session_id = sid
        
