#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

from radical.utils import Url
from sinon.agent import AgentException

from sinon.agent.agentlog.drivers import LocalAgentLog

#-----------------------------------------------------------------------------
#
class AgentLog(object):
    ''' The TaskSource object represent a local or remote stream of tasks
    '''

    #-------------------------------------------------------------------------
    #
    def __init__(self, agent_log_url):
        ''' Le constructeur tries to connect to the task source provided in
            task_source_url. If the source is invalid, it throw an exception.
        '''
        self._driver = self._load_driver(agent_log_url)

    #-------------------------------------------------------------------------
    #
    def _load_driver(self, agent_log_url):
        ''' Loads a tasks source driver based on the URL scheme.
        '''
        return LocalAgentLog(agent_log_url)

    #-------------------------------------------------------------------------
    #
    def info(self, message, suffix=None):
        self._driver.info(message, suffix)

    #-------------------------------------------------------------------------
    #
    def warning(self, message, suffix=None):
        self._driver.warning(message, suffix)

    #-------------------------------------------------------------------------
    #
    def error(self, message, suffix=None):
        self._driver.error(message, suffix)

    #-------------------------------------------------------------------------
    #
    def fatal(self, message, suffix=None):
        self._driver.fatal(message, suffix)

    def fatal_and_raise(self, message, suffix=None):
        self.fatal(message)
        raise AgentException(message, suffix)
