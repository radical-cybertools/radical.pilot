
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.saga as rs

from ...   import states             as rps
from ...   import constants          as rpc
from ...   import staging_directives as rpsd

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

        # we keep a cache of SAGA dir handles
        self._cache = dict()

        self.register_input(rps.TMGR_STAGING_OUTPUT_PENDING,
                            rpc.TMGR_STAGING_OUTPUT_QUEUE, self.work)

        # we don't need an output queue -- tasks will be final


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        for key in self._cache:
            self._cache[key].close()


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

            # no matter if we perform any staging or not, we will push the full
            # task info to the DB on the next advance, since the tasks will be
            # final
            task['$all']    = True
            task['control'] = None

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
            self._handle_task(task, actionables)


    # --------------------------------------------------------------------------
    #
    def _handle_task(self, task, actionables):

        uid = task['uid']

        src_context = {'pwd'      : task['task_sandbox'],       # !!!
                       'task'     : task['task_sandbox'],
                       'pilot'    : task['pilot_sandbox'],
                       'resource' : task['resource_sandbox']}
        tgt_context = {'pwd'      : os.getcwd(),                # !!!
                       'task'     : task['task_sandbox'],
                       'pilot'    : task['pilot_sandbox'],
                       'resource' : task['resource_sandbox']}

        # url used for cache (sandbox url w/o path)
        tmp      = rs.Url(task["task_sandbox"])
        tmp.path = '/'
        key      = str(tmp)

        if key not in self._cache:
            self._cache[key] = rs.filesystem.Directory(tmp,
                    session=self._session)
        saga_dir = self._cache[key]


        # Loop over all transfer directives and execute them.
        for sd in actionables:

          # action = sd['action']
            flags  = sd['flags']
            did    = sd['uid']
            src    = sd['source']
            tgt    = sd['target']

            self._prof.prof('staging_out_start', uid=uid, msg=did)

            self._log.debug('src: %s', src)
            self._log.debug('tgt: %s', tgt)

            src = rpsd.complete_url(src, src_context, self._log)
            tgt = rpsd.complete_url(tgt, tgt_context, self._log)

            self._log.debug('src: %s', src)
            self._log.debug('tgt: %s', tgt)

            # Check if the src is a folder, if true
            # add recursive flag if not already specified
            if saga_dir.is_dir(src.path):
                flags |= rs.filesystem.RECURSIVE

            # Always set CREATE_PARENTS
            flags |= rs.filesystem.CREATE_PARENTS

            self._log.debug('deed: %s: %s -> %s [%s]', saga_dir.url, src, tgt, flags)
            saga_dir.copy(src, tgt, flags=flags)
            self._prof.prof('staging_out_stop', uid=uid, msg=did)

        # all staging is done -- at this point the task is final
        task['state'] = task['target_state']
        self.advance(task, publish=True, push=True)


# ------------------------------------------------------------------------------

