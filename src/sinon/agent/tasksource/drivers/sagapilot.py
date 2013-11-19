#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

from sinon.db import Session
from sinon.agent import Task
from radical.utils import Url

import os
import json
from cgi import parse_qs

DRIVER = "SASGAPilot"

#-----------------------------------------------------------------------------
#
class SAGAPilot(object):


    #-------------------------------------------------------------------------
    #
    def __init__(self, logger, task_source_url):
        """
        """
        self.log = logger

        # extract hostname, session uid and pilot uid from 
        # the url.
        url = Url(task_source_url)

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

        self._db = Session.reconnect(sid=self.session_uid, 
                                     db_url=mongodb_url, 
                                     db_name=self.db_name)

        # get all tasks assigned to this pilot.
        self._tasks = []

        task_list = self._db.pilot_get_tasks(pilot_uid=self.pilot_uid)

        for entry in task_list:
            task = Task(uid=str(entry['_id']),
                        executable=entry['description']['Executable'],
                        arguments=entry['description']['Arguments'],
                        numcores='1',
                        stdout=None,
                        stderr=None)

            self._tasks.append(task)

        self.log.info("%s: Successfully opened location '%s'" % (DRIVER, task_source_url))
        self.log.info("%s: Loaded %s task descriptions from MongoDB into memory" % (DRIVER, len(self._tasks)))


    #-------------------------------------------------------------------------
    #
    def is_finite(self):
        # file sources are always finite
        return True

    #-------------------------------------------------------------------------
    #
    def num_tasks(self):
        """
        """
        return len(self._tasks)

    #-------------------------------------------------------------------------
    #
    def get_tasks(self, start_idx=0, limit=None):
        return self._tasks

    #-------------------------------------------------------------------------
    #
    def close(self):
        pass
