
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.utils as ru

from ...   import states             as rps
from ...   import constants          as rpc
from ...   import utils              as rpu

from ...staging_directives import expand_staging_directives

from .base import TMGRStagingOutputComponent



# ------------------------------------------------------------------------------
#
class Default(TMGRStagingOutputComponent):
    """
    This component performs all tmgr side output staging directives for compute
    tasks.  It gets tasks from the tmgr_staging_output_queue, in
    TMGR_STAGING_OUTPUT_PENDING state, will advance them to TMGR_STAGING_OUTPUT
    state while performing the staging, and then moves then to the respective
    final state.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        TMGRStagingOutputComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._stager = rpu.StagingHelper(self._log)

        self.register_input(rps.TMGR_STAGING_OUTPUT_PENDING,
                            rpc.PROXY_TASK_QUEUE,
                            qname=self._session.uid,
                            cb=self.work)

        # we don't need an output queue -- tasks will be final


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        if not isinstance(tasks, list):
            tasks = [tasks]

        self.advance(tasks, rps.TMGR_STAGING_OUTPUT, publish=True, push=False)

        # we first filter out any tasks which don't need any output staging, and
        # advance them again as a bulk.  We work over the others one by one, and
        # advance them individually, to avoid stalling from slow staging ops.

        no_staging_tasks = list()
        staging_tasks    = list()

        for task in tasks:

            # we only handle staging for tasks which end up in `DONE` state
            target_state = task.get('target_state')
            if target_state and target_state != rps.DONE:
                self._log.debug('skip staging for %s', task['uid'])
                no_staging_tasks.append(task)
                continue

            # check if we have any staging directives to be enacted in this
            # component
            actionables = list()
            for sd in task['description'].get('output_staging', []):

                if sd['action'] == rpc.TRANSFER:
                    actionables.append(sd)

            if actionables:
                staging_tasks.append([task, actionables])
            else:
                no_staging_tasks.append(task)


        if no_staging_tasks:

            # nothing to stage -- transition into final state.
            for task in no_staging_tasks:
                task['state'] = task['target_state']
            self.advance(no_staging_tasks, publish=True, push=True)

        for task,actionables in staging_tasks:
            try:
                self._handle_task(task, actionables)
                self.advance(task, publish=True, push=True)
            except:
                self._log.exception("staging error")
                self.advance(task, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def _handle_task(self, task, actionables):

        uid = task['uid']

        src_context = {'pwd'      : task['task_sandbox'],       # !
                       'client'   : task['client_sandbox'],
                       'task'     : task['task_sandbox'],
                       'pilot'    : task['pilot_sandbox'],
                       'session'  : task['session_sandbox'],
                       'resource' : task['resource_sandbox'],
                       'endpoint' : task['endpoint_fs']}
        tgt_context = {'pwd'      : task['client_sandbox'],     # !
                       'client'   : task['client_sandbox'],
                       'task'     : task['task_sandbox'],
                       'pilot'    : task['pilot_sandbox'],
                       'session'  : task['session_sandbox'],
                       'resource' : task['resource_sandbox'],
                       'endpoint' : task['endpoint_fs']}

        # url used for cache (sandbox url w/o path)
        tmp      = ru.Url(task["task_sandbox"])
        tmp.path = '/'
        key      = str(tmp)

        actionables = expand_staging_directives(actionables,
                                            src_context, tgt_context, self._log)

        # Loop over all transfer directives and execute them.
        for sd in actionables:
            self._prof.prof('staging_in_start', uid=uid, msg=sd['uid'])
            self._stager.handle_staging_directive(sd)
            self._prof.prof('staging_in_stop', uid=uid, msg=sd['uid'])

        # all staging is done -- at this point the task is final
        task['state'] = task['target_state']
        self.advance(task, publish=True, push=True)


# ------------------------------------------------------------------------------

