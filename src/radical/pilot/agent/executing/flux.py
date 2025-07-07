
__copyright__ = 'Copyright 2013-2020, http://radical.rutgers.edu'
__license__   = 'MIT'

import threading as mt

from collections import defaultdict

import radical.utils as ru

from ...   import states as rps
from ..    import LaunchMethod
from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
class Flux(AgentExecutingComponent) :

    _shell = ru.which('bash') or '/bin/sh'

    # --------------------------------------------------------------------------
    #
    def initialize(self):
        '''
        This components has 3 strands of activity (threads):

          - the main thread listens for incoming tasks from the scheduler, and
            pushes them toward the watcher thread;
          - an event listener thread listens for flux events which signify task
            state updates, and pushes those events also to the watcher thread;
          - the watcher thread matches events and tasks, enacts state updates,
            and pushes completed tasks toward output staging.

        NOTE: we get tasks in *AGENT_SCHEDULING* state, and enact all
              further state changes in this component.
        '''

        super().initialize()

        # translate Flux states to RP states
        self._event_map = {'submit'   : None,   # rps.AGENT_SCHEDULING,
                           'depend'   : None,
                           'alloc'    : rps.AGENT_EXECUTING_PENDING,
                         # 'start'    : rps.AGENT_EXECUTING,
                           'cleanup'  : None,
                           'finish'   : rps.AGENT_STAGING_OUTPUT_PENDING,
                           'release'  : 'unschedule',
                           'free'     : None,
                           'clean'    : None,
                           'priority' : None,
                           'exception': rps.FAILED,
                          }

        lm_cfg = self.session.rcfg.launch_methods.get('FLUX')
        lm_cfg['pid']       = self.session.cfg.pid
        lm_cfg['reg_addr']  = self.session.cfg.reg_addr
        self._lm            = LaunchMethod.create('FLUX', lm_cfg, self._rm.info,
                                                  self._log, self._prof)

        # we only start the flux backend here
        self._lm.start_flux(event_cb=self._handle_event_cb,
                            create_cb=self._create_cb)

        # local state management
        self._tasks       = dict()             # task_id -> task
        self._events      = defaultdict(list)  # flux_id -> [events]
        self._events_lock = mt.Lock()


        self._task_count = 0


    # --------------------------------------------------------------------------
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def _lm_fail_task(self, tasks, state):
        '''
        This will be called by the launch method in case a task error was
        detected during launch, before handing the task to flux
        '''

        for task in tasks:
            self._log.error('flux launch failed for %s: %s', task['uid'], state)

        self.advance_tasks(tasks, state=rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def get_task(self, tid):

        return self._tasks.get(tid)


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, task):

        # FIXME: clarify how to cancel tasks in Flux
        pass


    # --------------------------------------------------------------------------
    #
    def _create_cb(self, task):

        self._log.debug('create exec script: %s', task['uid'])
        _, exec_path = self._create_exec_script(self._lm, task)

        return exec_path


    # --------------------------------------------------------------------------
    #
    def _handle_event_cb(self, task_id, event):

        ename = event.name
        state = self._event_map.get(ename)

        self._log.debug('flux event: %s: %s [%s]', task_id, ename, state)
        self._log.debug_3('          : %s', str(event.context))

        task = self._tasks.get(task_id)

        if ename == 'lm_failed':
            self.advance_tasks(task, rps.FAILED, publish=True, push=False)
            return

        if state is None:
            return

        if state == rps.AGENT_STAGING_OUTPUT_PENDING:

            task['exit_code'] = event.context.get('status', 1)
            if task['exit_code']: task['target_state'] = rps.FAILED
            else                : task['target_state'] = rps.DONE

            # FIXME: run post-launch commands here.  Alas, this is
            #        synchronous, and thus potentially rather slow.
            tid  = task['uid']
            cmds = task['description'].get('post_launch')
            if cmds:
                for cmd in cmds:
                    self._log.debug('post-launch %s: %s', task['uid'], cmd)
                    out, err, ret = ru.sh_callout(cmd, shell=True,
                                                  cwd=task['task_sandbox_path'])
                    self._log.debug('post-launch %s: %s [%s][%s]',
                                                             tid, ret, out, err)
                    if ret:
                        self.advance_tasks(task, rps.FAILED,
                                           publish=True, push=False)
                        return

            # on completion, push toward output staging
            self.advance_tasks(task, state, ts=event.timestamp,
                               publish=True, push=True)

        elif state == 'unschedule':

            # free task resources
            self._prof.prof('unschedule_start', uid=task['uid'])
            self._prof.prof('unschedule_stop',  uid=task['uid'])  # ?
          # self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

        else:
            # otherwise only push a state update
            self.advance_tasks(task, state, ts=event.timestamp,
                               publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        for task in tasks:
            self._tasks[task['uid']] = task

        self._lm.submit_tasks(tasks)


# ------------------------------------------------------------------------------

