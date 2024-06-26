
__copyright__ = 'Copyright 2013-2020, http://radical.rutgers.edu'
__license__   = 'MIT'


from collections import defaultdict
from functools   import partial

import radical.utils as ru

from ...   import states as rps

from ..    import LaunchMethod
from ..    import ResourceManager
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

        # we get an instance of the resource manager (init from registry info)
        rm_name  = self.session.rcfg.resource_manager
        self._rm = ResourceManager.create(rm_name,
                                          self.session.cfg,
                                          self.session.rcfg,
                                          self._log, self._prof)

        lm_cfg  = self.session.rcfg.launch_methods.get('FLUX')
        lm_cfg['pid']       = self.session.cfg.pid
        lm_cfg['reg_addr']  = self.session.cfg.reg_addr
        self._lm            = LaunchMethod.create('FLUX', lm_cfg, self._rm.info,
                                                  self._log, self._prof)
        # local state management
        self._tasks      = defaultdict(dict)  # dict[partion_id][flux_id] = task
        self._task_count = 0


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, uid):

        # FIXME: clarify how to cancel tasks in Flux
        pass


    # --------------------------------------------------------------------------
    #
    def _job_event_cb(self, partition_id, flux_id, event):

        task = self._tasks[partition_id].get(flux_id)
        if not task:
            self._log.error('no task for flux job %s: %s', flux_id, event.name)


        flux_state = event.name  # event: flux_id, flux_state
        state = self._event_map.get(flux_state)

        self._log.debug('flux event: %s: %s [%s]', flux_id, flux_state, state)

        if state is None:
            # ignore this state transition
            self._log.debug('ignore flux event %s:%s' %
                            (task['uid'], flux_state))

        # FIXME: how to get actual event transition timestamp?
        ts = event.timestamp

        self._log.debug('task state: %s [%s]', state, event)

        if state == rps.AGENT_STAGING_OUTPUT_PENDING:

            task['exit_code'] = event.context.get('status', 1)

            if task['exit_code']:
                task['target_state'] = rps.FAILED
            else:
                task['target_state'] = rps.DONE

            # on completion, push toward output staging
            self.advance_tasks(task, state, ts=ts, publish=True, push=True)

        elif state == 'unschedule':

            # free task resources
            self._prof.prof('unschedule_start', uid=task['uid'])
            self._prof.prof('unschedule_stop',  uid=task['uid'])  # ?
          # self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

        else:
            # otherwise only push a state update
            self.advance_tasks(task, state, ts=ts, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        # round robin on available flux partitions
        parts = defaultdict(list)
        for task in tasks:

            partition_id = task['description']['partition']

            if partition_id is None:
                partition_id = self._task_count % self._lm.n_partitions
                self._task_count += 1

            parts[partition_id].append(task)
            task['description']['environment']['RP_PARTITION_ID'] = partition_id

        for partition_id, partition_tasks in parts.items():

            part = self._lm.get_partition(partition_id)
            jds  = [self.task_to_spec(task) for task in partition_tasks]
            jids = part.submit_jobs([jd for jd in jds])
            self._log.debug('submitted tasks: %s', jids)

            for task, flux_id in zip(partition_tasks, jids):

                self._log.debug('submitted task %s -> %s', task['uid'], flux_id)

                md = task['description'].get('metadata') or dict()
                md['flux_id'] = flux_id
                task['description']['metadata'] = md

                self._tasks[partition_id][flux_id] = task

                event_cb = partial(self._job_event_cb, partition_id)

                part.attach_jobs([flux_id], event_cb)
                self._log.debug('handle %s: %s', task['uid'], flux_id)


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

        spec = {
            'tasks': [{
                'slot' : 'task',
                'count': {
                    'per_slot': 1
                },
                'command': [self._shell, '-c', command],
            }],
            'attributes': {
                'system': {
                    'cwd'     : sbox,
                    'duration': 0,
                },
            },
            'version': 1,
            'resources': [{
                'count': td['ranks'],
                'type' : 'slot',
                'label': 'task',
                'with' : [{
                    'count': td['cores_per_rank'],
                    'type' : 'core'
              # }, {
              #     'count': int(td['gpus_per_rank'] or 0),
              #     'type' : 'gpu'
                }]
            }]
        }

        if td['gpus_per_rank']:

            gpr = td['gpus_per_rank']

            if gpr != int(gpr):
                raise ValueError('flux does not support on-integer GPU count')

            spec['resources'][0]['with'].append({
                    'count': int(gpr),
                    'type' : 'gpu'})

        return spec


# ------------------------------------------------------------------------------

