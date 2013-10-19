#!/usr/bin/env python
# encoding: utf-8

from pymongo import MongoClient

#-----------------------------------------------------------------------------
#

class Session():

    def __init__(self, db_url):
        self._client = MongoClient(db_url)
        self._db = self._client.sinon
        self._collection = None

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

    def delete(self):
        """ Remove session and all associated collections from the DB.
        """
        if self._collection is None:
            raise Exception("No active session.")
        self._collection.remove()


    def _reconnect(self, sid):
        pass
        

    def _create(self, sid):
        self._collection = self._db["%s" % sid]
        
