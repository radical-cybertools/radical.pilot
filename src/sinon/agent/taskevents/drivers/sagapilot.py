#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

from cgi import parse_qs
import json
import datetime
import threading

#from pymongo import MongoClient
#from bson.objectid import ObjectId

from sinon.db import Session

from radical.utils import Url

DRIVER = "SAGAPilot"

#-----------------------------------------------------------------------------
#
class SAGAPilot(object):

    #-------------------------------------------------------------------------
    #
    def __init__(self, logger, task_results_url):
        """
        """
        self.log = logger

        # extract hostname, session uid and pilot uid from 
        # the url.
        url = Url(task_results_url)

        self.db_name     = None
        self.session_uid = None
        self.pilot_uid   = None

        for key, val in parse_qs(url.query).iteritems():
            if key == 'session':
                self.session_uid = val[0]
            if key == 'pilot':
                self.pilot_uid = val[0]
            if key == 'dbname':
                self.db_name = val[0]

        if self.session_uid is None or self.pilot_uid is None or self.db_name is None:
            raise Exception("--event URL doesn't define 'session', 'pilot' or 'dbname'")

        # connect to MongoDB using sinon's DB layer
        mongodb_url = "mongodb://%s" % url.host
        if url.port is not None:
            mongodb_url += ":%s" % url.port

        self._db = Session(db_url=mongodb_url, db_name=self.db_name)
  
    #-------------------------------------------------------------------------
    #
    def __del__(self):
        # nothing to do
        pass

    #-------------------------------------------------------------------------
    #
    def close(self):
        # nothing to do
        pass

    #-------------------------------------------------------------------------
    #
    def put(self, origin_type, origin_id, event, value):
        ''' Publish a new task event.
        '''
        if origin_type == "AGENT":
            # agent-level event
            if event == "STATECHANGE":
                self._db.pilot_set_state(pilot_uid=self.pilot_uid, state=value)
            else:
                print "unknown event"

        elif origin_type == "TASK":
            pass
