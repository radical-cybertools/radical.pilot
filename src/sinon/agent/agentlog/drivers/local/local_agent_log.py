#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

from .logger import getLogger

#-----------------------------------------------------------------------------
#
class LocalAgentLog(object):

    #-------------------------------------------------------------------------
    #
    def __init__(self, agent_log_url):
        pass

    def info(self, message, suffix=None):
        getLogger(suffix).info(message)

    def warning(self, message, suffix=None):
        getLogger(suffix).warning(message)

    def error(self, message, suffix=None):
        getLogger(suffix).error(message)

    def fatal(self, message, suffix=None):
        getLogger(suffix).fatal(message)
