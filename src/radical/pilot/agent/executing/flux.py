
__copyright__ = 'Copyright 2013-2020, http://radical.rutgers.edu'
__license__   = 'MIT'


import time
import queue
import threading as mt

import radical.utils as ru

from ...   import states    as rps
from ...   import constants as rpc

from ..    import LaunchMethod
from ..    import ResourceManager
from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
class Flux(AgentExecutingComponent) :

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__(self, cfg, session)


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

        AgentExecutingComponent.initialize(self)

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
            ret = True

        elif state == 'unschedule':

            # free task resources
            self._prof.prof('unschedule_start', uid=task['uid'])
            self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

        else:
            # otherwise only push a state update
            self.advance_tasks(task, state, ts=ts, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        for task in tasks:

            flux_id = task['description']['metadata']['flux_id']
            assert flux_id not in self._tasks
            self._tasks[flux_id] = task


            fut = self._lm.fh.attach_jobs([flux_id], self._job_event_cb)
            self._log.debug('handle %s: %s', task['uid'], flux_id)


# ------------------------------------------------------------------------------

