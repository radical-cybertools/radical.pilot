
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

        self._watcher = mt.Thread(target=self._timed)
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

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        for task in tasks:

            try:
                self._handle_task(task)

            except Exception:
                # append the startup error to the tasks stderr.  This is
                # not completely correct (as this text is not produced
                # by the task), but it seems the most intuitive way to
                # communicate that error to the application/user.
                self._log.exception("error running Task")
                if task['stderr'] is None:
                    task['stderr'] = ''
                task['stderr'] += '\nPilot cannot start task:\n'
                task['stderr'] += '\n'.join(ru.get_exception_trace())

                # can't rely on the executor base to free the task resources
                self._prof.prof('unschedule_start', uid=task['uid'])
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

                self.advance(task, rps.FAILED, publish=True, push=False)

        with self._tasks_lock:
            self._tasks.extend(tasks)


    # --------------------------------------------------------------------------
    #
    def _handle_task(self, task):

        now = time.time()

        # assert(t['description']['executable'].endswith('sleep'))
        task['to_finish'] = now + float(task['description']['arguments'][0])

        uid = task['uid']
        self._prof.prof('exec_start',      uid=uid)
        self._prof.prof('exec_ok',         uid=uid)
        self._prof.prof('task_start',      uid=uid)
        self._prof.prof('task_exec_start', uid=uid)
        self._prof.prof('app_start',       uid=uid)


    # --------------------------------------------------------------------------
    #
    def _timed(self):

        while not self._terminate.is_set():

            time.sleep(self._delay)

            with self._tasks_lock:
                now = time.time()
                to_finish   = [t for t in self._tasks if t['to_finish'] <= now]
                self._tasks = [t for t in self._tasks if t['to_finish'] >  now]

            for task in to_finish:
                uid = task['uid']
                task['target_state'] = 'DONE'
                self._prof.prof('app_stop',         uid=uid)
                self._prof.prof('task_exec_stop',   uid=uid)
                self._prof.prof('task_stop',        uid=uid)
                self._prof.prof('exec_stop',        uid=uid)
                self._prof.prof('unschedule_start', uid=uid)
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

            self.advance(to_finish, rps.AGENT_STAGING_OUTPUT_PENDING,
                                    publish=True, push=True)


# ------------------------------------------------------------------------------

