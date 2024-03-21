
__copyright__ = 'Copyright 2013-2020, http://radical.rutgers.edu'
__license__   = 'MIT'


from ...   import states as rps

from ..    import LaunchMethod
from ..    import ResourceManager
from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
class Flux(AgentExecutingComponent) :

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
        self._lm            = LaunchMethod.create('FLUX', lm_cfg,
                                                  self.session.cfg,
                                                  self._log, self._prof)
        # local state management
        self._tasks  = dict()


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, uid):

        # FIXME: clarify how to cancel tasks in Flux
        pass


    # --------------------------------------------------------------------------
    #
    def _job_event_cb(self, flux_id, event):

        task = self._tasks.get(flux_id)
        if not task:
            self._log.error('no task for flux job %s: %s', flux_id, event.name)

        flux_state = event.name  # event: flux_id, flux_state
        state = self._event_map.get(flux_state)

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

        # FIXME: need actual job description, obviously
        jds = [self.task_to_spec(task) for task in tasks]
        self._log.debug('submit tasks: %s', [jd for jd in jds])
        jids = self._lm.fh.submit_jobs([jd for jd in jds])
        self._log.debug('submitted tasks')

        for task, flux_id in zip(tasks, jids):

            self._log.debug('submitted task %s -> %s', task['uid'], flux_id)

            md = task['description'].get('metadata') or dict()
            md['flux_id'] = flux_id
            task['description']['metadata'] = md

            self._tasks[flux_id] = task

            self._lm.fh.attach_jobs([flux_id], self._job_event_cb)
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
                'command': ['/bin/sh', '-c', command],
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

