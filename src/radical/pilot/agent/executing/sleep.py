
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time

import threading     as mt

import radical.utils as ru

from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
class Sleep(AgentExecutingComponent) :

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__ (self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._pwd = os.getcwd()

        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher (rpc.AGENT_UNSCHEDULE_PUBSUB)

        self._terminate  = mt.Event()
        self._tasks_lock = ru.RLock()
        self._tasks      = list()
        self._delay      = 0.1

        self._timed = mt.Thread(target=self._timed)
        self._timed.daemon = True
        self._timed.start()


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        self._terminate.set()
        self._timed.join()


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_EXECUTING, publish=True, push=False)

        now = time.time()
        for t in units:
          # assert(t['description']['executable'].endswith('sleep'))
            t['to_finish'] = now + float(t['description']['arguments'][0])

        for t in units:
            uid = t['uid']
            self._prof.prof('exec_start',    uid=uid)
            self._prof.prof('exec_ok',       uid=uid)
            self._prof.prof('cu_start',      uid=uid)
            self._prof.prof('cu_cd_done',    uid=uid)
            self._prof.prof('cu_exec_start', uid=uid)
            self._prof.prof('app_start',     uid=uid)

        with self._tasks_lock:
            self._tasks.extend(units)


    # --------------------------------------------------------------------------
    #
    def _timed(self):

        while not self._terminate.is_set():

            time.sleep(self._delay)

            with self._tasks_lock:
                now = time.time()
                to_finish   = [t for t in self._tasks if t['to_finish'] <= now]
                self._tasks = [t for t in self._tasks if t['to_finish'] >  now]

            for t in to_finish:
                uid = t['uid']
                t['target_state'] = 'DONE'
                self._prof.prof('app_stop',         uid=uid)
                self._prof.prof('cu_exec_stop',     uid=uid)
                self._prof.prof('cu_stop',          uid=uid)
                self._prof.prof('exec_stop',        uid=uid)
                self._prof.prof('unschedule_start', uid=uid)
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, t)

            self.advance(to_finish, rps.AGENT_STAGING_OUTPUT_PENDING,
                                    publish=True, push=True)


# ------------------------------------------------------------------------------

