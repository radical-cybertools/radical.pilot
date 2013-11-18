#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

import json
import datetime
import threading

DRIVER = "JSONFile"

#-----------------------------------------------------------------------------
#
class JSONFile(object):

    #-------------------------------------------------------------------------
    #
    def __init__(self, logger, task_results_url):
        # extract the file path and open the file
        # and read the task data
        self._tasks = list()
        self.log = logger

        self.file_path = task_results_url.path
        # a global lock so file access can be synchronized
        self._putlock = threading.Lock()

        self.log.info("%s: Successfully created/opened events file '%s'" % (DRIVER, self.file_path))

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
    def put_agent_statechange(self, newstate):
        # synchronize file access
        with self._putlock:
            events_file = open(self.file_path, 'a')
            # create a JSON dictionary for the task result
            result = {
                'type': 'event',
                'timestamp': str(datetime.datetime.now()),
                'origin': "SELF",
                'event': "AGENT_STATECHANGE",
                'value': newstate
            }
            events_file.write(json.dumps(result)+'\n')
            events_file.close()

    #-------------------------------------------------------------------------
    #
    def put(self, origin, event, value):
        ''' Publish a new task event.
        '''
        # synchronize file access
        with self._putlock:
            events_file = open(self.file_path, 'a')
            # create a JSON dictionary for the task result
            result = {
                'type': 'event',
                'timestamp': str(datetime.datetime.now()),
                'origin': origin,
                'event': event,
                'value': value
            }
            events_file.write(json.dumps(result)+'\n')
            events_file.close()
