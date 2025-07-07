
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
        self._stager = rpu.StagingHelper(log=self._log)

        self.register_input(rps.AGENT_STAGING_OUTPUT_PENDING,
                            rpc.AGENT_STAGING_OUTPUT_QUEUE, self.work)

        self.register_output(rps.TMGR_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_COLLECTING_QUEUE)



    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance(tasks, rps.AGENT_STAGING_OUTPUT, publish=True, push=False)

        # we first filter out any tasks which don't need any input staging, and
        # advance them again as a bulk.  We work over the others one by one, and
        # advance them individually, to avoid stalling from slow staging ops.

        no_staging_tasks = list()
        staging_tasks    = list()

        for task in ru.as_list(tasks):

            try:
                uid = task['uid']

                # From here on, any state update will hand control over to the
                # tmgr again.  The next task update should thus push *all* task
                # details, not only state.
                task['$all']    = True
                task['control'] = 'tmgr_pending'

                self._log.debug('=== staging task %s: %s', uid, task['state'])
                import pprint
                self._log.debug('=== staging task %s: %s', uid, pprint.pformat(task))

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

            except Exception as e:
                self._log.exception('staging prep error')
                task['exception']        = repr(e)
                task['exception_detail'] = '\n'.join(ru.get_exception_trace())

                self.advance(task, rps.FAILED)


        if no_staging_tasks:
            self.advance(no_staging_tasks, rps.TMGR_STAGING_OUTPUT_PENDING,
                                           publish=True, push=True)

        for task, actionables in staging_tasks:
            try:
                self._handle_task_staging(task, actionables)

            except Exception as e:
                self._log.exception('staging error')
                task['exception']        = repr(e)
                task['exception_detail'] = '\n'.join(ru.get_exception_trace())

                self.advance(task, rps.FAILED)


    # --------------------------------------------------------------------------
    #
    def _handle_task_stdio(self, task):

        sbox = task.get('task_sandbox_path')
        uid  = task['uid']

        self._log.debug('out %s: %s', uid, sbox)

        self._prof.prof('staging_stdout_start', uid=uid)
        self._log.debug('out %s: %s', uid, task.get('stdout_file'))

        # TODO: disable this at scale?
        if task.get('stdout_file') and os.path.isfile(task['stdout_file']):
            with ru.ru_open(task['stdout_file'], 'r', errors='ignore') \
                 as stdout_f:
                try:
                    txt = ru.as_string(stdout_f.read())
                except UnicodeDecodeError:
                    txt = "task stdout is binary -- use file staging"

                task['stdout'] += rpu.tail(txt)

        self._prof.prof('staging_stdout_stop',  uid=uid)
        self._prof.prof('staging_stderr_start', uid=uid)

        # TODO: disable this at scale?
        if task.get('stderr_file') and os.path.isfile(task['stderr_file']):
            with ru.ru_open(task['stderr_file'], 'r', errors='ignore') \
                 as stderr_f:
                try:
                    txt = ru.as_string(stderr_f.read())
                except UnicodeDecodeError:
                    txt = "task stderr is binary -- use file staging"

                task['stderr'] += rpu.tail(txt)

        self._prof.prof('staging_stderr_stop', uid=uid)
        self._prof.prof('staging_uprof_start', uid=uid)

      # task_prof = '%s/%s.prof' % (sbox, uid)
      # if os.path.isfile(task_prof):
      #     pids = {}
      #     try:
      #         with ru.ru_open(task_prof, 'r', errors='ignore') as prof_f:
      #             txt = ru.as_string(prof_f.read())
      #             for line in txt.split("\n"):
      #                 if not line:
      #                     continue
      #                 if line[0] == '#':
      #                     continue
      #                 ts, event, comp, tid, _uid, state, msg = \
      #                                                      line.split(',')
      #                 self._prof.prof(ts=float(ts), event=event,
      #                                 comp=comp, tid=tid, uid=_uid,
      #                                 state=state, msg=msg)
      #                 # collect task related PIDs
      #                 if 'RP_LAUNCH_PID' in msg:
      #                     pids['launch_pid'] = int(msg.split('=')[1])
      #                 elif 'RP_RANK_PID' in msg:
      #                     epid_msg, rpid_msg = msg.split(':')
      #                     pids.setdefault('exec_pid', []) \
      #                         .append(int(epid_msg.split('=')[1]))
      #                     pids.setdefault('rank_pid', [])\
      #                         .append(int(rpid_msg.split('=')[1]))
      #     except Exception as e:
      #         self._log.error("Pre/Post profile read failed: `%s`" % e)
      #
      #     if pids:
      #         # keep process IDs within metadata in the task description
      #         if not task['description'].get('metadata'):
      #             task['description']['metadata'] = {}
      #         task['description']['metadata'].update(pids)

        self._prof.prof('staging_uprof_stop', uid=uid)
        self._prof.prof('staging_ofile_start', uid=uid)

        task_ofile = '%s/%s.ofiles' % (sbox, uid)
        if os.path.isfile(task_ofile):
            try:
                with ru.ru_open(task_ofile, 'r', errors='ignore') as fin:
                    task['ofiles'] = [l.strip() for l in fin.readlines()]
            except Exception as e:
                self._log.error("Pre/Post ofile read failed: `%s`" % e)

        self._prof.prof('staging_ofile_stop', uid=uid)


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
        session_sandbox  = ru.Url(task['session_sandbox'])
        resource_sandbox = ru.Url(task['resource_sandbox'])
        endpoint_fs      = ru.Url(task['endpoint_fs'])

        task_sandbox.schema     = 'file'
        pilot_sandbox.schema    = 'file'
        session_sandbox.schema  = 'file'
        resource_sandbox.schema = 'file'
        endpoint_fs.schema      = 'file'

        task_sandbox.host       = 'localhost'
        pilot_sandbox.host      = 'localhost'
        session_sandbox.host    = 'localhost'
        resource_sandbox.host   = 'localhost'
        endpoint_fs.host        = 'localhost'

        src_context = {'pwd'      : str(task_sandbox),       # !!!
                       'task'     : str(task_sandbox),
                       'pilot'    : str(pilot_sandbox),
                       'session'  : str(session_sandbox),
                       'resource' : str(resource_sandbox),
                       'endpoint' : str(endpoint_fs)}
        tgt_context = {'pwd'      : str(task_sandbox),       # !!!
                       'task'     : str(task_sandbox),
                       'pilot'    : str(pilot_sandbox),
                       'session'  : str(session_sandbox),
                       'resource' : str(resource_sandbox),
                       'endpoint' : str(endpoint_fs)}


        # we can now handle the actionable staging directives
        for sd in actionables:

            action = sd['action']
            flags  = sd['flags']
            did    = sd['uid']
            src    = sd['source']
            tgt    = sd['target']

            self._prof.prof('staging_out_start', uid=uid, msg=did)

            # agent stager only handles local actions
            if action not in [rpc.COPY, rpc.LINK, rpc.MOVE]:
                self._prof.prof('staging_in_skip', uid=uid, msg=did)
                continue

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
            assert src.schema == 'file', 'staging src must be file://'

            if action in [rpc.COPY, rpc.LINK, rpc.MOVE]:
                assert tgt.schema == 'file', 'staging tgt expected as file://'

            self._stager.handle_staging_directive({'source': src,
                                                   'target': tgt,
                                                   'action': action,
                                                   'flags' : flags})


            self._prof.prof('staging_out_stop', uid=uid, msg=did)

        # all agent staging is done -- pass on to tmgr output staging
        self.advance(task, rps.TMGR_STAGING_OUTPUT_PENDING,
                           publish=True, push=True)


# ------------------------------------------------------------------------------

