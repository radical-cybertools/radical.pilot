
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

        self.register_publisher(rpc.AGENT_UNSCHEDULE_PUBSUB)

        self._terminate  = mt.Event()
        self._tasks_lock = mt.RLock()
        self._tasks      = list()
        self._delay      = 1.0

        self._watcher = mt.Thread(target=self._collect)
        self._watcher.daemon = True
        self._watcher.start()


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        self._terminate.set()
        self._watcher.join()


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        if not isinstance(tasks, list):
            tasks = [tasks]

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        now = time.time()
        for t in tasks:
            uid = t['uid']
            self._prof.prof('exec_start',      uid=uid)
            self._prof.prof('exec_ok',         uid=uid)
            self._prof.prof('task_start',      uid=uid)
            self._prof.prof('task_exec_start', uid=uid)
            self._prof.prof('app_start',       uid=uid)

            t['to_finish'] = now + float(t['description']['arguments'][0])

        self._log.debug('=== started new tasks        : %d', len(tasks))

        with self._tasks_lock:
            self._tasks.extend(tasks)


    # --------------------------------------------------------------------------
    #
    def _collect(self):

        while not self._terminate.is_set():

            with self._tasks_lock:

                to_finish   = list()
                to_continue = list()
                now         = time.time()

                for task in self._tasks:
                    if task['to_finish'] <= now: to_finish.append(task)
                    else                       : to_continue.append(task)

                self._tasks = to_continue

            if not to_finish:
                time.sleep(self._delay)
                continue

            uids = list()
            for t in to_finish:
                uid = t['uid']
                uids.append(uid)
                t['target_state'] = 'DONE'
                self._prof.prof('app_stop',         uid=uid)
                self._prof.prof('task_exec_stop',   uid=uid)
                self._prof.prof('task_stop',        uid=uid)
                self._prof.prof('exec_stop',        uid=uid)
                self._prof.prof('unschedule_start', uid=uid)

            self._log.debug('=== collected                : %d', len(to_finish))

            self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, to_finish)

            self.advance(to_finish, rps.AGENT_STAGING_OUTPUT_PENDING,
                                    publish=True, push=True)


# ------------------------------------------------------------------------------

