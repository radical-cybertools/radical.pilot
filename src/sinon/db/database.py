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
        s = Session(db_url)
        s._create(sid)
        return s

    @staticmethod
    def reconnect(db_url, sid):
        s = Session(db_url)
        s._reconnect(sid)
        return s

    @property
    def session_id(self):
        return self._session_id

    def delete(self):
        """ Remove session and all associated collections from the DB.
        """
        if self._collection is None:
            raise Exception("No active session.")
        self._collection.drop()

    def _reconnect(self, sid):
        if sid not in self._db.collection_names():
            raise Exception("Session ID '%s' doesn't exists in DB." % sid)
        self._collection = self._db["%s" % sid]
        self._session_id = sid

    def _create(self, sid):
        if sid in self._db.collection_names():
            raise Exception("Session ID '%s' already exists in DB." % sid)
        self._collection = self._db["%s" % sid]
        self._collection.insert({'created': True})
        self._session_id = sid
        
