
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import errno
import shutil

import radical.utils as ru

from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentStagingOutputComponent

from ...staging_directives import complete_url


# ------------------------------------------------------------------------------
#
class Default(AgentStagingOutputComponent):
    """
    This component performs all agent side output staging directives for compute
    tasks.  It gets tasks from the agent_staging_output_queue, in
    AGENT_STAGING_OUTPUT_PENDING state, will advance them to
    AGENT_STAGING_OUTPUT state while performing the staging, and then moves then
    to the TMGR_STAGING_OUTPUT_PENDING state, which at the moment requires the
    state change to be published to MongoDB (no push into a queue).

    Note that this component also collects stdout/stderr of the tasks (which
    can also be considered staging, really).
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentStagingOutputComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._pwd = os.getcwd()

        self.register_input(rps.AGENT_STAGING_OUTPUT_PENDING,
                            rpc.AGENT_STAGING_OUTPUT_QUEUE, self.work)

        # we don't need an output queue -- tasks are picked up via mongodb
        self.register_output(rps.TMGR_STAGING_OUTPUT_PENDING, None)  # drop


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        if not isinstance(tasks, list):
            tasks = [tasks]

        self.advance(tasks, rps.AGENT_STAGING_OUTPUT, publish=True, push=False)

        # we first filter out any tasks which don't need any input staging, and
        # advance them again as a bulk.  We work over the others one by one, and
        # advance them individually, to avoid stalling from slow staging ops.

        no_staging_tasks = list()
        staging_tasks    = list()

        for task in tasks:

            uid = task['uid']

            # From here on, any state update will hand control over to the tmgr
            # again.  The next task update should thus push *all* task details,
            # not only state.
            task['$all']    = True
            task['control'] = 'tmgr_pending'

            # we always dig for stdout/stderr
            self._handle_task_stdio(task)

            # NOTE: all tasks get here after execution, even those which did not
            #       finish successfully.  We do that so that we can make
            #       stdout/stderr available for failed tasks (see
            #       _handle_task_stdio above).  But we don't need to perform any
            #       other staging for those tasks, and in fact can make them
            #       final.
            if task['target_state'] != rps.DONE \
                    and not task['description'].get('stage_on_error'):
                task['state'] = task['target_state']
                self._log.debug('task %s skips staging: %s', uid, task['state'])
                no_staging_tasks.append(task)
                continue

            # check if we have any staging directives to be enacted in this
            # component
            actionables = list()
            for sd in task['description'].get('output_staging', []):
                if sd['action'] in [rpc.LINK, rpc.COPY, rpc.MOVE]:
                    actionables.append(sd)

            if actionables:
                # this task needs some staging
                staging_tasks.append([task, actionables])
            else:
                # this task does not need any staging at this point, and can be
                # advanced
                task['state'] = rps.TMGR_STAGING_OUTPUT_PENDING
                no_staging_tasks.append(task)

        if no_staging_tasks:
            self.advance(no_staging_tasks, publish=True, push=True)

        for task,actionables in staging_tasks:
            self._handle_task_staging(task, actionables)


    # --------------------------------------------------------------------------
    #
    def _handle_task_stdio(self, task):

        sandbox = task['task_sandbox_path']
        uid     = task['uid']

        self._prof.prof('staging_stdout_start', uid=uid)

        # TODO: disable this at scale?
        if task.get('stdout_file') and os.path.isfile(task['stdout_file']):
            with open(task['stdout_file'], 'r') as stdout_f:
                try:
                    txt = ru.as_string(stdout_f.read())
                except UnicodeDecodeError:
                    txt = "task stdout is binary -- use file staging"

                task['stdout'] += rpu.tail(txt)

        self._prof.prof('staging_stdout_stop',  uid=uid)
        self._prof.prof('staging_stderr_start', uid=uid)

        # TODO: disable this at scale?
        if task.get('stderr_file') and os.path.isfile(task['stderr_file']):
            with open(task['stderr_file'], 'r') as stderr_f:
                try:
                    txt = ru.as_string(stderr_f.read())
                except UnicodeDecodeError:
                    txt = "task stderr is binary -- use file staging"

                task['stderr'] += rpu.tail(txt)

            # to help with ID mapping, also parse for PRTE output:
            # [batch3:122527] JOB [3673,4] EXECUTING
            with open(task['stderr_file'], 'r') as stderr_f:

                for line in stderr_f.readlines():
                    line = line.strip()
                    if not line:
                        continue
                    if line[0] == '[' and line.endswith('EXECUTING'):
                        elems = line.replace('[', '').replace(']', '').split()
                        tid   = elems[2]
                        self._log.info('PRTE IDMAP: %s:%s' % (tid, uid))

                task['stderr'] += rpu.tail(txt)

        self._prof.prof('staging_stderr_stop', uid=uid)
        self._prof.prof('staging_uprof_start', uid=uid)

        task_prof = "%s/%s.prof" % (sandbox, uid)
        if os.path.isfile(task_prof):
            try:
                with open(task_prof, 'r') as prof_f:
                    txt = ru.as_string(prof_f.read())
                    for line in txt.split("\n"):
                        if line:
                            ts, event, comp, tid, _uid, state, msg = \
                                                                 line.split(',')
                            self._prof.prof(ts=float(ts), event=event,
                                            comp=comp, tid=tid, uid=_uid,
                                            state=state, msg=msg)
            except Exception as e:
                self._log.error("Pre/Post profile read failed: `%s`" % e)

        self._prof.prof('staging_uprof_stop', uid=uid)


    # --------------------------------------------------------------------------
    #
    def _handle_task_staging(self, task, actionables):

        uid = task['uid']

        # By definition, this compoentn lives on the pilot's target resource.
        # As such, we *know* that all staging ops which would refer to the
        # resource now refer to file://localhost, and thus translate the task,
        # pilot and resource sandboxes into that scope.  Some assumptions are
        # made though:
        #
        #   * paths are directly translatable across schemas
        #   * resource level storage is in fact accessible via file://
        #
        # FIXME: this is costly and should be cached.

        task_sandbox     = ru.Url(task['task_sandbox'])
        pilot_sandbox    = ru.Url(task['pilot_sandbox'])
        resource_sandbox = ru.Url(task['resource_sandbox'])

        task_sandbox.schema     = 'file'
        pilot_sandbox.schema    = 'file'
        resource_sandbox.schema = 'file'

        task_sandbox.host       = 'localhost'
        pilot_sandbox.host      = 'localhost'
        resource_sandbox.host   = 'localhost'

        src_context = {'pwd'      : str(task_sandbox),       # !!!
                       'task'     : str(task_sandbox),
                       'pilot'    : str(pilot_sandbox),
                       'resource' : str(resource_sandbox)}
        tgt_context = {'pwd'      : str(task_sandbox),       # !!!
                       'task'     : str(task_sandbox),
                       'pilot'    : str(pilot_sandbox),
                       'resource' : str(resource_sandbox)}

        # we can now handle the actionable staging directives
        for sd in actionables:

            action = sd['action']
            flags  = sd['flags']
            did    = sd['uid']
            src    = sd['source']
            tgt    = sd['target']

            self._prof.prof('staging_out_start', uid=uid, msg=did)

            assert(action in [rpc.COPY, rpc.LINK, rpc.MOVE, rpc.TRANSFER]), \
                              'invalid staging action'

            # we only handle staging which does *not* include 'client://' src or
            # tgt URLs - those are handled by the tmgr staging components
            if src.startswith('client://'):
                self._log.debug('skip staging for src %s', src)
                self._prof.prof('staging_out_skip', uid=uid, msg=did)
                continue

            if tgt.startswith('client://'):
                self._log.debug('skip staging for tgt %s', tgt)
                self._prof.prof('staging_out_skip', uid=uid, msg=did)
                continue

            # Fix for when the target PATH is empty
            # we assume current directory is the task staging 'task://'
            # and we assume the file to be copied is the base filename
            # of the source
            if tgt is None: tgt = ''
            if tgt.strip() == '':
                tgt = 'task:///{}'.format(os.path.basename(src))
            # Fix for when the target PATH is exists *and* it is a folder
            # we assume the 'current directory' is the target folder
            # and we assume the file to be copied is the base filename
            # of the source
            elif os.path.exists(tgt.strip()) and os.path.isdir(tgt.strip()):
                tgt = os.path.join(tgt, os.path.basename(src))

            src = complete_url(src, src_context, self._log)
            tgt = complete_url(tgt, tgt_context, self._log)

            # Currently, we use the same schema for files and folders.
            assert(src.schema == 'file'), 'staging src must be file://'

            if action in [rpc.COPY, rpc.LINK, rpc.MOVE]:
                assert(tgt.schema == 'file'), 'staging tgt expected as file://'

            # SAGA will take care of dir creation - but we do it manually
            # for local ops (copy, link, move)
            if flags & rpc.CREATE_PARENTS and action != rpc.TRANSFER:
                tgtdir = os.path.dirname(tgt.path)
                if tgtdir != task_sandbox.path:
                    self._log.debug("mkdir %s", tgtdir)
                    rpu.rec_makedir(tgtdir)

            if   action == rpc.COPY:
                try:
                    shutil.copytree(src.path, tgt.path)
                except OSError as exc:
                    if exc.errno == errno.ENOTDIR:
                        shutil.copy(src.path, tgt.path)
                    else:
                        raise

            elif action == rpc.LINK:
                # Fix issue/1513 if link source is file and target is folder
                # should support POSIX standard where link is created
                # with the same name as the source
                if os.path.isfile(src.path) and os.path.isdir(tgt.path):
                    os.symlink(src.path,
                               os.path.join(tgt.path,
                                            os.path.basename(src.path)))
                else:  # default behavior
                    os.symlink(src.path, tgt.path)
            elif action == rpc.MOVE: shutil.move(src.path, tgt.path)
            elif action == rpc.TRANSFER: pass
                # This is currently never executed. Commenting it out.
                # Uncomment and implement when uploads directly to remote URLs
                # from tasks are supported.
                # FIXME: we only handle srm staging right now, and only for
                #        a specific target proxy. Other TRANSFER directives are
                #        left to tmgr output staging.  We should use SAGA to
                #        attempt all staging ops which do not target the client
                #        machine.
                # if tgt.schema == 'srm':
                #     # FIXME: cache saga handles
                #     srm_dir = rs.filesystem.Directory('srm://proxy/?SFN=bogus')
                #     srm_dir.copy(src, tgt)
                #     srm_dir.close()
                # else:
                #     self._log.error('no transfer for %s -> %s', src, tgt)
                #     self._prof.prof('staging_out_fail', uid=uid, msg=did)
                #     raise NotImplementedError('unsupported transfer %s' % tgt)

            self._prof.prof('staging_out_stop', uid=uid, msg=did)

        # all agent staging is done -- pass on to tmgr output staging
        self.advance(task, rps.TMGR_STAGING_OUTPUT_PENDING,
                           publish=True, push=False)


# ------------------------------------------------------------------------------

