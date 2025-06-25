
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
class NOOP(AgentExecutingComponent) :

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

        self.advance_tasks(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        for task in tasks:

            try:
                self._prof.prof('task_start', uid=task['uid'])
                self._handle_task(task)

            except Exception as e:
                self._log.exception("error running Task")
                task['exception']        = repr(e)
                task['exception_detail'] = '\n'.join(ru.get_exception_trace())

                # can't rely on the executor base to free the task resources
                self._prof.prof('unschedule_start', uid=task['uid'])
                self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

                self.advance_tasks(task, rps.FAILED, publish=True, push=False)

        with self._tasks_lock:
            self._tasks.extend(tasks)


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, task):

        raise NotImplementedError('no cancellation support in noop executor')


    # --------------------------------------------------------------------------
    #
    def _handle_task(self, task):

        now  = time.time()
        td   = task['description']
        args = td.get('arguments', [0])

        task['deadline'] = now
        if 'sleep' in td['executable']:
            try:
                task['deadline'] = now + float(args[0])
            except:
                pass

        uid = task['uid']
        self._prof.prof('task_run_start', uid=uid)
        self._prof.prof('task_run_ok',    uid=uid)
        self._prof.prof('launch_start',   uid=uid)
        self._prof.prof('exec_start',     uid=uid)
        self._prof.prof('rank_start',     uid=uid)


    # --------------------------------------------------------------------------
    #
    def _collect(self):

        while not self._terminate.is_set():

            to_finish   = list()
            to_continue = list()
            now         = time.time()

            with self._tasks_lock:

                for task in self._tasks:
                    if task['deadline'] <= now: to_finish.append(task)
                    else                      : to_continue.append(task)

                self._tasks = to_continue

            if not to_finish:
                time.sleep(self._delay)
                continue

            for task in to_finish:
                uid = task['uid']
                task['target_state'] = 'DONE'

                self._prof.prof('rank_stop',        uid=uid)
                self._prof.prof('exec_stop',        uid=uid)
                self._prof.prof('launch_stop',      uid=uid)
                self._prof.prof('task_run_stop',    uid=uid)
                self._prof.prof('unschedule_start', uid=uid)

            self._log.debug('collected                : %d', len(to_finish))

            self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, to_finish)
            self.advance_tasks(to_finish, rps.AGENT_STAGING_OUTPUT_PENDING,
                                          publish=True, push=True)


    # --------------------------------------------------------------------------
    #
    def control_cb(self, topic, msg):

        self._log.info('control_cb [%s]: %s', topic, msg)

        cmd = msg.get('cmd')

        # FIXME RPC: already handled in the component base class
        if cmd == 'cancel_tasks':

            # FIXME: clarify how to cancel tasks
            pass


# ------------------------------------------------------------------------------

