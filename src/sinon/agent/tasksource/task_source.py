#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

from radical.utils import Url

from sinon.agent.tasksource.drivers import JSONFile
from sinon.agent.tasksource.drivers import SAGAPilot

#-----------------------------------------------------------------------------
#
class TaskSource(object):
    ''' The TaskSource object represent a local or remote stream of tasks
    '''

    #-------------------------------------------------------------------------
    #
    def __init__(self, logger, task_source_url):
        ''' Le constructeur tries to connect to the task source provided in
            task_source_url. If the source is invalid, it throw an exception.
        '''
        self.log = logger
        self._driver = self._load_driver(task_source_url)

    #-------------------------------------------------------------------------
    #
    def __del__(self):
        self.close()

    #-------------------------------------------------------------------------
    #
    def _load_driver(self, task_source_url):
        ''' Loads a tasks source driver based on the URL scheme.
        '''
        try:
            ts_url = Url(task_source_url)
            self.log.info("Trying to load driver for task source '%s'" % str(ts_url))

            if ts_url.scheme == "file":
                driver = JSONFile(self.log, ts_url)
                self.log.info("Successfully loaded task source driver '%s'" % driver.__class__.__name__)
                return driver
            elif ts_url.scheme == "sagapilot":
                driver = SAGAPilot(self.log, ts_url)
                self.log.info("Successfully loaded task source driver '%s'" % driver.__class__.__name__)
                return driver
            else:
                raise Exception("No driver for URL scheme '%s://'" % ts_url.scheme)
        except Exception, ex:
            self.log.fatal_and_raise("Failed to load a suitable driver: %s" % ex)

    #-------------------------------------------------------------------------
    #
    @property
    def is_finite(self):
        ''' Returns True if the task source is finite, meaning that the task
            source won't dynamically generate more tasks during the course of
            its existence.
        '''
        return self._driver.is_finite()

    #-------------------------------------------------------------------------
    #
    @property
    def num_tasks(self):
        ''' Returns the number of tasks available at the task source. If
            is_finite is True, this can obviously change.
        '''
        return self._driver.num_tasks()

    #-------------------------------------------------------------------------
    #
    def get_tasks(self, start_idx=0, limit=None):
        ''' Returns up to 'limit' tasks from the task source, starting at
            index 'start_idx'.
        '''
        return self._driver.get_tasks(start_idx, limit)

    #-------------------------------------------------------------------------
    #
    def close(self):
        ''' Closes the task source. Subsequent calls will result in an error.
        '''
        return self._driver.close()

