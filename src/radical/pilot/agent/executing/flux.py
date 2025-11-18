
__copyright__ = 'Copyright 2013-2020, http://radical.rutgers.edu'
__license__   = 'MIT'

import copy
import time

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
        # NOTE: only map to states for which output channels are defined
        self._event_map = {
                         # 'submit'   : rps.AGENT_SCHEDULING,
                         # 'depend'   : rps.AGENT_SCHEDULING_PENDING,
                         # 'alloc'    : rps.AGENT_EXECUTING_PENDING,
                         # 'start'    : rps.AGENT_EXECUTING,
                           'cleanup'  : None,
                           'finish'   : rps.AGENT_STAGING_OUTPUT_PENDING,
                         # 'release'  : 'unschedule',
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

        # local state management
        self._tasks       = dict()             # task_id -> task
        self._events      = defaultdict(list)  # flux_id -> [events]
        self._events_lock = mt.Lock()

        self._task_count = 0

        # we only start the flux backend here
        self._lm.start_flux(event_cb=self._handle_event_cb)


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
        self._lm.cancel_task(task)


    # --------------------------------------------------------------------------
    #
    def _create_spec(self, task):

        tid = task['uid']

        self._log.debug('create spec: %s', tid)

        td     = task['description']
        uid    = task['uid']
        sbox   = task['task_sandbox_path']
        stdout = td.get('stdout') or '%s/%s.out' % (sbox, uid)
        stderr = td.get('stderr') or '%s/%s.err' % (sbox, uid)

        td['sandbox'] = sbox
        td['stdout']  = stdout
        td['stderr']  = stderr

        task['stdout'] = ''
        task['stderr'] = ''

        task['stdout_file'] = stdout
        task['stderr_file'] = stderr

        self._prof.prof('task_create_exec_start', uid=uid)
        _, exec_path = self._create_exec_script(self._lm, task)
        self._prof.prof('task_create_exec_ok', uid=uid)

        spec_dict = copy.deepcopy(td)
        spec_dict['uid']        = uid
        spec_dict['executable'] = '/bin/sh'
        spec_dict['arguments']  = ['-c', exec_path]

        self._prof.prof('task_to_spec_start', uid=uid)
        ret = ru.flux.spec_from_dict(spec_dict)
        self._prof.prof('task_to_spec_stop', uid=uid)

        return ret


    # --------------------------------------------------------------------------
    #
    def _handle_event_cb(self, task_id, event):

        ename = event.name
        state = None
        push  = True

        self._log.debug('flux event: %s: %s', task_id, ename)
        self._log.debug_3('          : %s', str(event.context))

        task = self._tasks.get(task_id)

        # handle some special events, fallback to _event_map otherwise
        if ename == 'start':
            # start task timeout handling, no further action
            self.handle_timeout(task)

        elif ename == 'unschedule':
            # free task resources, no further action
            self._prof.prof('unschedule_start', uid=task['uid'])
            self._prof.prof('unschedule_stop',  uid=task['uid'])  # ?

        elif ename == 'lm_failed':
            self._log.error('flux launch failed for %s', task_id)
            state = rps.FAILED
            push  = False

        elif ename == 'exception' and \
             event.context['type'] in ['cancel', 'timeout']:

            state = rps.AGENT_STAGING_OUTPUT_PENDING
            task['target_state'] = rps.CANCELED

        else:
            state = self._event_map.get(ename)


        if state is None:
            # no further state handling needed
            return

        self._log.debug('flux event mapped: %s -> %s', ename, state)

        # handle some states specifically
        if state == rps.AGENT_STAGING_OUTPUT_PENDING:

            if not task.get('target_state'):
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

        # otherwise we just advance to the found state
        self.advance_tasks(task, state, ts=event.timestamp,
                           publish=True, push=push)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        n_parts = self._rm.info.n_partitions
        parts   = defaultdict(dict)  # part_id -> {task_id: task}

        for task in tasks:

            tid = task['uid']
            self._tasks[tid] = task

            try:
                # FIXME: pre_launch commands are synchronous and thus
                #        potentially slow.
                self._prof.prof('work_0', uid=tid)

                cmds = task['description'].get('pre_launch')
                if cmds:
                    sbox = task['task_sandbox_path']
                    ru.rec_makedir(sbox)

                    for cmd in cmds:
                        self._log.debug('pre-launch %s: %s', task['uid'], cmd)
                        out, err, ret = ru.sh_callout(cmd, shell=True,
                                              cwd=task['task_sandbox_path'])
                        self._log.debug('pre-launch %s: %s [%s][%s]',
                                        tid, ret, out, err)

                        if ret:
                            raise RuntimeError('cmd failed: %s' % cmd)

                self._prof.prof('work_1', uid=task['uid'])
                part_id = task['description']['partition']
                if part_id is None:
                    part_id = self._task_count % n_parts
                    self._task_count += 1

                self._prof.prof('work_2', uid=task['uid'])
                self._log.debug('task %s on partition %s', task['uid'], part_id)
                task['description']['environment']['RP_PARTITION_ID'] = part_id

                parts[part_id][tid] = self._create_spec(task)
                self._prof.prof('work_3', uid=task['uid'])

            except:
                self._log.exception('LM flux submit failed for %s', tid)
                self.advance_tasks(task, rps.FAILED, publish=True, push=False)

        self._lm.submit_tasks(parts)


# ------------------------------------------------------------------------------

