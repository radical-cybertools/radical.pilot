#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

from radical.utils import Url

from sinon.agent.taskresults.drivers import JSONFile
from sinon.agent.taskresults.drivers import SAGAPilot

#-----------------------------------------------------------------------------
#
class TaskResults(object):
    ''' The TaskSource object represent a local or remote stream of tasks
    '''

    #-------------------------------------------------------------------------
    #
    def __init__(self, logger, task_results_url):
        ''' Le constructeur tries to connect to the task results location
            provided in task_results_url. If the location is invalid, it
            throws an exception.
        '''
        self.log = logger
        self._driver = self._load_driver(task_results_url)

    #-------------------------------------------------------------------------
    #
    def __del__(self):
        self.close()

    #-------------------------------------------------------------------------
    #
    def _load_driver(self, task_results_url):
        ''' Loads a tasks results driver based on the URL scheme.
        '''
        try:
            ts_url = Url(task_results_url)
            self.log.info("Trying to load driver for task results location '%s'" % str(ts_url))

            if ts_url.scheme == "file":
                driver = JSONFile(self.log, ts_url)
                self.log.info("Successfully loaded task results driver '%s'" % driver.__class__.__name__)
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
    def put(self, task_id, cmdline, exit_code, working_directory,
            stdout, stderr, start_time, stop_time, runtime, execution_locations):
        ''' Publish a new task result.
        '''
        return self._driver.put(task_id, cmdline, exit_code, working_directory,
                                stdout, stderr, start_time, stop_time, runtime, execution_locations)

    #-------------------------------------------------------------------------
    #
    def close(self):
        ''' Closes the task resoult location. 
            Subsequent calls will result in an error.
        '''
        return self._driver.close()
