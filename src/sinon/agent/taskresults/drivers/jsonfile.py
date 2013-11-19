#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

#from __future__ import with_statement
import json
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

        # try to open the file in the task_source_url
        file_path = task_results_url.path
        self.result_file = open(file_path, 'a')

        # a global lock so file access can be synchronized
        self._putlock = threading.Lock()

        self.log.info("%s: Successfully created/opened result file '%s'" % (DRIVER, file_path))

    #-------------------------------------------------------------------------
    #
    def __del__(self):
        try:
            self.close()
        except Exception, ex:
            # already close
            pass

    #-------------------------------------------------------------------------
    #
    def close(self):
        self.result_file.flush()
        self.result_file.close()
        self.log.info("Flushing and closing results file.")

    #-------------------------------------------------------------------------
    #
    def put(self, task_id, cmdline, exit_code, working_directory,
            stdout, stderr, start_time, stop_time, runtime, execution_locations):
        ''' Publish a new task result.
        '''
        with self._putlock:
            # create a JSON dictionary for the task result
            result = {
                'type': 'result',
                'task_id': task_id,
                'cmdline': cmdline,
                'exit_code': exit_code,
                'working_directory': working_directory,
                'stdout': stdout,
                'stderr': stderr,
                'execution_location': execution_locations,
                'start_time': start_time,
                'stop_time': stop_time,
                'runtime': runtime
            }

            self.result_file.write(json.dumps(result)+'\n')
            self.result_file.flush()
