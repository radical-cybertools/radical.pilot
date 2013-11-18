#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

import multiprocessing.queues


#-----------------------------------------------------------------------------
#
class Result(object):
    ''' encapsulates the result of a task execution
    '''
    def __init__(self, job_uid, rc, exec_host, cmdline, launch_method,
                 environment, working_directory, start_time, end_time, runtime):
        self.rc = rc
        self.job_uid = job_uid
        self.cmdline = cmdline
        self.exec_host = exec_host
        self.launch_method = launch_method
        self.environment = environment
        self.working_directory = working_directory
        self.start_time = start_time
        self.end_time = end_time
        self.runtime = runtime

    def __repr__(self):
        dct = {'job_uid':           self.job_uid,
               'rc':                self.rc,
               'cmdline':           self.cmdline,
               'exec_host':         self.exec_host,
               'launch_method':     self.launch_method,
               'environment':       self.environment,
               'working_directory': self.working_directory,
               'start_time':        self.start_time,
               'end_time':          self.end_time,
               'runtime':           self.runtime
               }
        return str(dct)


#-----------------------------------------------------------------------------
#
class ResultQueue(multiprocessing.queues.JoinableQueue):
    ''' ResultQueue
    '''
    #-------------------------------------------------------------------------
    #
    def __init__(self, maxsize=0):
        ''' le constructeur
        '''
        # init superclass
        super(ResultQueue, self).__init__(maxsize=maxsize)
