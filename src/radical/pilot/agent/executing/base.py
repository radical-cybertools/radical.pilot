
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import time

import threading          as mt

import radical.utils      as ru

from ... import states    as rps
from ... import agent     as rpa
from ... import constants as rpc
from ... import utils     as rpu


# ------------------------------------------------------------------------------
# 'enum' for RP's spawner types
EXECUTING_NAME_POPEN   = 'POPEN'
EXECUTING_NAME_FLUX    = 'FLUX'
EXECUTING_NAME_SLEEP   = 'SLEEP'


# ------------------------------------------------------------------------------
#
class AgentExecutingComponent(rpu.AgentComponent):
    '''
    Manage the creation of Task processes, and watch them until they are
    completed (one way or the other).  The spawner thus moves the task from
    PendingExecution to Executing, and then to a final state (or PendingStageOut
    of course).
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        super().__init__(cfg, session)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Spawner
    #
    @classmethod
    def create(cls, cfg, session):

        # Make sure that we are the base-class!
        if cls != AgentExecutingComponent:
            raise TypeError('Factory only available to base class!')

        name = session.rcfg.agent_spawner

        from .popen    import Popen
        from .flux     import Flux
        from .sleep    import Sleep

        impl = {
            EXECUTING_NAME_POPEN: Popen,
            EXECUTING_NAME_FLUX : Flux,
            EXECUTING_NAME_SLEEP: Sleep,
        }

        if name not in impl:
            raise ValueError('AgentExecutingComponent %s unknown' % name)

        return impl[name](cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):


        rm_name  = self.session.rcfg.resource_manager
        self._rm = rpa.ResourceManager.create(rm_name,
                                              self.session.cfg,
                                              self.session.rcfg,
                                              self._log, self._prof)

        self._pwd      = os.path.realpath(os.getcwd())
        self.sid       = self.session.uid
        self.resource  = self.session.cfg.resource
        self.rsbox     = self.session.cfg.resource_sandbox
        self.ssbox     = self.session.cfg.session_sandbox
        self.psbox     = self.session.cfg.pilot_sandbox
        self.gtod      = '$RP_PILOT_SANDBOX/gtod'
        self.prof      = '$RP_PILOT_SANDBOX/prof'

        # if so configured, let the tasks know what to use as tmp dir
        self._task_tmp = self.session.rcfg.get('task_tmp',
                                               os.environ.get('TMP', '/tmp'))

        if self.psbox.startswith(self.ssbox):
            self.psbox = '$RP_SESSION_SANDBOX%s'  % self.psbox[len(self.ssbox):]
        if self.ssbox.startswith(self.rsbox):
            self.ssbox = '$RP_RESOURCE_SANDBOX%s' % self.ssbox[len(self.rsbox):]
        if self.ssbox.endswith(self.sid):
            self.ssbox = '%s$RP_SESSION_ID/'      % self.ssbox[:-len(self.sid)]

        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher (rpc.AGENT_UNSCHEDULE_PUBSUB)

        self._to_tasks  = list()
        self._to_lock   = mt.Lock()
        self._to_thread = mt.Thread(target=self._to_watcher)
        self._to_thread.daemon = True
        self._to_thread.start()


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        raise NotImplementedError('work is not implemented')


    # --------------------------------------------------------------------------
    #
    def control_cb(self, topic, msg):

        self._log.info('command_cb [%s]: %s', topic, msg)

        cmd = msg['cmd']
        arg = msg['arg']

        # FIXME RPC: already handled in the component base class
        if cmd == 'cancel_tasks':

            self._log.info('cancel_tasks command (%s)', arg)
            for tid in arg['uids']:
                self.cancel_task(tid)


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, uid):

        raise NotImplementedError('cancel_task is not implemented')


    # --------------------------------------------------------------------------
    #
    def _to_watcher(self):
        '''
        watch the set of tasks for which timeouts are defined.  If the timeout
        passes and the tasks are still active, kill the task via
        `self._cancel_task(task)`.  That has to be implemented by al executors.
        '''

        while True:

            # check once per second at most
            time.sleep(1)

            now = time.time()
            with self._to_lock:

                # running tasks for next check
                to_list = list()
                for to, start, task in self._to_tasks:
                    if now - start > to:
                        self._prof.prof('task_timeout', uid=task['uid'])
                        self.cancel_task(uid=task['uid'])
                    else:
                        to_list.append([to, start, task])

                self._to_tasks = to_list


    # --------------------------------------------------------------------------
    #
    def handle_timeout(self, task):

        to = task['description'].get('timeout', 0.0)

        if to > 0.0:
            with self._to_lock:
                self._to_tasks.append([to, time.time(), task])


    # --------------------------------------------------------------------------
    #
    def advance_tasks(self, tasks, state, publish, push, ts=None):
        '''
        sort tasks into different buckets, depending on their origin.
        That origin will determine where tasks which completed execution
        and end up here will be routed to:

          - client: state update to update worker
          - raptor: state update to `STATE_PUBSUB`
          - agent : state update to `STATE_PUBSUB`

        a fallback is not in place to enforce the specification of the
        `origin` attributes for tasks.
        '''


        buckets = {'client': list(),
                   'raptor': list(),
                   'agent' : list()}

        for task in ru.as_list(tasks):
            buckets[task['origin']].append(task)

        # we want any task which has a `raptor_id` set to show up in raptor's
        # result callbacks
        if state != rps.AGENT_EXECUTING:
            for task in ru.as_list(tasks):
                if task['description'].get('raptor_id'):
                    if task not in buckets['raptor']:
                        buckets['raptor'].append(task)

        if buckets['client']:
            self.advance(buckets['client'], state=state,
                                            publish=publish, push=push, ts=ts)

        if buckets['raptor']:
            self.advance(buckets['raptor'], state=state,
                                            publish=publish, push=False, ts=ts)
            self.publish(rpc.STATE_PUBSUB, {'cmd': 'raptor_state_update',
                                            'arg': buckets['raptor']})

        if buckets['agent']:
            self.advance(buckets['agent'], state=state,
                                            publish=publish, push=False, ts=ts)


# ------------------------------------------------------------------------------

