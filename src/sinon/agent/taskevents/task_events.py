#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

from radical.utils import Url
from sinon.agent.taskevents.drivers import JSONFile
from sinon.agent.taskevents.drivers import SAGAPilot


#-----------------------------------------------------------------------------
#
class TaskEvents(object):
    ''' The TaskEvents object represent a local or remote location which
        accepts task events.
    '''

    #-------------------------------------------------------------------------
    #
    def __init__(self, logger, task_events_url):
        ''' Le constructeur tries to connect to the task events location
            provided in task_events_url. If the location is invalid, it
            throws an exception.
        '''
        self.log = logger
        self._driver = self._load_driver(task_events_url)

    #-------------------------------------------------------------------------
    #
    def __del__(self):
        self.close()

    #-------------------------------------------------------------------------
    #
    def _load_driver(self, task_events_url):
        ''' Loads a tasks events driver based on the URL scheme.
        '''
        try:
            ts_url = Url(task_events_url)
            self.log.info("Trying to load driver for task events location '%s'" % str(ts_url))

            if ts_url.scheme == "file":
                driver = JSONFile(self.log, ts_url)
                self.log.info("Successfully loaded task events driver '%s'" % driver.__class__.__name__)
                return driver
            elif ts_url.scheme == "sagapilot":
                driver = SAGAPilot(self.log, ts_url)
                self.log.info("Successfully loaded task events driver '%s'" % driver.__class__.__name__)
                return driver
            else:
                raise Exception("No driver for URL scheme '%s://'" % ts_url.scheme)
        except Exception, ex:
            self.log.fatal_and_raise("Failed to load a suitable driver: %s" % ex)

    #-------------------------------------------------------------------------
    #
    def put(self, origin, event, value):
        ''' Publish a new task event.
        '''
        return self._driver.put(origin, event, value)

    def put_pilot_statechange(self, newstate):
        """Publish an agent state change event.
        """
        return self._driver.put_pilot_statechange(newstate)

    #-------------------------------------------------------------------------
    #
    def close(self):
        ''' Closes the task events location.
            Subsequent calls to put() will result in an error.
        '''
        return self._driver.close()
