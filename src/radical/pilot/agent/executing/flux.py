
__copyright__ = 'Copyright 2013-2020, http://radical.rutgers.edu'
__license__   = 'MIT'

import time
import copy

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
                           'start'    : rps.AGENT_EXECUTING,
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

        # register state cb
        self._log.debug('register flux state cb: %s',
                        len(self._lm.partitions))

        if not self._lm.partitions:
            raise RuntimeError('no partitions found')

        for part in self._lm.partitions:
            self._log.debug('register flux state cb: %s', part.handle)
            part.handle.register_cb(self._job_event_cb)

        # local state management
        self._tasks  = dict()             # flux_id -> task
        self._events = defaultdict(list)  # flux_id -> [events]
        self._idmap  = dict()             # flux_id -> task_id

        self._task_count = 0


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, task):

        # FIXME: clarify how to cancel tasks in Flux
        pass


    # --------------------------------------------------------------------------
    #
    def _job_event_cb(self, tid, event):

        self._log.debug('flux event: %s: %s', tid, event.name)

        while True:
            task = self._tasks.get(tid)
            if not task:
                task = self._tasks.get(self._idmap.get(tid))
            if task:
                break
            time.sleep(0.1)

        if not task:
            self._log.error('no task for flux job %s: %s %s', tid,
                            event.name, list(self._tasks.keys()))
            self._events[tid].append(event)

        else:
            self._handle_event(task, tid, event)


    # --------------------------------------------------------------------------
    #
    def _handle_event(self, task, flux_id, event):

        ename = event.name
        state = self._event_map.get(ename)

        if ename == 'alloc':
            self._log.debug('map fluxid: %s: %s', flux_id, task['uid'])

        self._log.debug('flux event: %s: %s [%s]', flux_id, ename, state)

        if state is None:
            return

        if state == rps.AGENT_STAGING_OUTPUT_PENDING:

            task['exit_code'] = event.context.get('status', 1)
            if task['exit_code']: task['target_state'] = rps.FAILED
            else                : task['target_state'] = rps.DONE

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

        try:
            # round robin on available flux partitions
            parts = defaultdict(list)
            for task in tasks:

                part_id = task['description']['partition']

                if part_id is None:
                    part_id = self._task_count % len(self._lm.partitions)
                    self._task_count += 1

                parts[part_id].append(task)
                task['description']['environment']['RP_PARTITION_ID'] = part_id
                self._log.debug('task %s on partition %s', task['uid'], part_id)

            for part_id, part_tasks in parts.items():

                part  = self._lm.partitions[part_id]
                specs = list()
                for task in part_tasks:
                    tid = task['uid']
                    self._tasks[tid] = task
                    specs.append(self.task_to_spec(task))

                tids = [task['uid'] for task in part_tasks]
                fids = part.handle.submit(specs)

                for fid, tid in zip(fids, tids):
                    self._idmap[fid] = tid

                self._log.debug('%s: submitted %d tasks: %s', part.uid,
                                len(tids), tids)

        except Exception as e:
            self._log.exception('flux submit failed: %s', e)
            raise


    # --------------------------------------------------------------------------
    #
    def task_to_spec(self, task):

        td     = task['description']
        uid    = task['uid']
        sbox   = task['task_sandbox_path']
        stdout = td.get('stdout') or '%s/%s.out' % (sbox, uid)
        stderr = td.get('stderr') or '%s/%s.err' % (sbox, uid)

        task['stdout'] = ''
        task['stderr'] = ''

        task['stdout_file'] = stdout
        task['stderr_file'] = stderr

        _, exec_path = self._create_exec_script(self._lm, task)

        command = '%(cmd)s 1>%(out)s 2>%(err)s' % {'cmd': exec_path,
                                                   'out': stdout,
                                                   'err': stderr}
        spec_dict = copy.deepcopy(td)
        spec_dict['uid']        = uid
        spec_dict['executable'] = '/bin/sh'
        spec_dict['arguments']  = ['-c', command]

        return ru.flux.spec_from_dict(spec_dict)


    # --------------------------------------------------------------------------
    #
    def control_cb(self, topic, msg):

        self._log.info('command_cb [%s]: %s', topic, msg)

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        # FIXME RPC: already handled in the component base class
        if cmd == 'task_execution_done':

            self._log.info('task_execution_done command (%s)', arg)
            self._prof.prof('task_execution_done')

        else:

            super().control_cb(topic, msg)


# ------------------------------------------------------------------------------

