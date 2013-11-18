#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

import multiprocessing.queues


#-----------------------------------------------------------------------------
#
class Task(object):

    def __init__(self, uid, executable, arguments, numcores, stdout, stderr):
        self._executable = executable
        self._arguments = arguments
        self._stdout = stdout
        self._stderr = stderr
        self._numcores = numcores
        self._uid = uid

    @property
    def uid(self):
        return self._uid

    @property
    def numcores(self):
        return self._numcores

    @property
    def executable(self):
        return self._executable

    @property
    def arguments(self):
        return self._arguments

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stderr


#-----------------------------------------------------------------------------
#
class TaskQueue(multiprocessing.queues.JoinableQueue):
    ''' TaskQueue
    '''
    #-------------------------------------------------------------------------
    #
    def __init__(self, maxsize=0):
        ''' le constructeur
        '''
        # init superclass
        super(TaskQueue, self).__init__(maxsize=maxsize)
