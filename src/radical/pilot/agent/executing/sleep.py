
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import stat
import time
import Queue
import signal
import tempfile
import traceback
import subprocess

import threading     as mt

import radical.utils as ru

from .... import pilot     as rp
from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentExecutingComponent


# ==============================================================================
#
class Sleep(AgentExecutingComponent) :

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__ (self, cfg, session)

        self._terminate = mt.Event()


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self._pwd = os.getcwd()

        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher (rpc.AGENT_UNSCHEDULE_PUBSUB)

        self._delay      = 0.1
        self._tasks_lock = mt.RLock()
        self._tasks      = list()

        self._t = mt.Timer(self._delay, self._timed)
        self._t.start()


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self._log.debug(' === +%6d', len(units))
        self.advance(units, rps.AGENT_EXECUTING, publish=True, push=False)

        now = time.time()
        for t in units:
            assert(t['description']['executable'].endswith('sleep'))
            t['to_finish'] = now + float(t['description']['arguments'][0])

        for t in units:
            self._prof.prof('app_stop', uid=t['uid'])

        with self._tasks_lock:
            self._tasks.extend(units)


    # --------------------------------------------------------------------------
    #
    def _timed(self):


        with self._tasks_lock:
            self._log.debug(' === =%6d', len(self._tasks))
            now = time.time()
            to_finish   = [t for t in self._tasks if t['to_finish'] >= now]
            self._tasks = [t for t in self._tasks if t['to_finish'] <  now]

        for t in to_finish:
            self._prof.prof('app_stop', uid=t['uid'])

        self._log.debug(' === -%6d', len(to_finish))
        self.advance(to_finish, rps.AGENT_STAGING_OUTPUT_PENDING,
                                publish=True, push=True)

        self._t = mt.Timer(self._delay, self._timed)
        self._t.start()


# ------------------------------------------------------------------------------

