
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import errno
import os
import shutil
import tarfile

import radical.utils as ru

from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import AgentStagingInputComponent

from ...staging_directives import complete_url


# ------------------------------------------------------------------------------
#
class Default(AgentStagingInputComponent):
    """
    This component performs all agent side input staging directives for compute
    tasks.  It gets tasks from the agent_staging_input_queue, in
    AGENT_STAGING_INPUT_PENDING state, will advance them to AGENT_STAGING_INPUT
    state while performing the staging, and then moves then to the
    AGENT_SCHEDULING_PENDING state, into the agent_scheduling_queue.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentStagingInputComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._pwd = os.getcwd()
        self._stager = rpu.StagingHelper(log=self._log)

        self.register_input(rps.AGENT_STAGING_INPUT_PENDING,
                            rpc.AGENT_STAGING_INPUT_QUEUE, self.work)

        self.register_output(rps.AGENT_SCHEDULING_PENDING,
                             rpc.AGENT_SCHEDULING_QUEUE)


    # --------------------------------------------------------------------------
    #
    def _work(self, tasks):

        # we first filter out any tasks which don't need any input staging, and
        # advance them again as a bulk.  We work over the others one by one, and
        # advance them individually, to avoid stalling from slow staging ops.

        no_staging_tasks = list()
        staging_tasks    = list()

        for task in ru.as_list(tasks):

            # check if we have any staging directives to be enacted in this
            # component
            actionables = list()
            for sd in task['description'].get('input_staging', []):

                if sd['action'] in [rpc.LINK, rpc.COPY, rpc.MOVE,
                                    rpc.TARBALL, rpc.DOWNLOAD]:
                    actionables.append(sd)

            if actionables:
                staging_tasks.append([task, actionables])
            else:
                no_staging_tasks.append(task)

        if no_staging_tasks:
            self.advance(no_staging_tasks, rps.AGENT_SCHEDULING_PENDING,
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
        # FIXME: URL creation and manipulation is costly and should be cached

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
            did    = sd['uid']
            src    = sd['source']
            tgt    = sd['target']
            flags  = sd.get('flags', 0)

            self._prof.prof('staging_in_start', uid=uid, msg=did)

            # agent stager only handles local actions
            if action not in [rpc.COPY, rpc.LINK, rpc.MOVE, rpc.DOWNLOAD]:
                self._prof.prof('staging_in_skip', uid=uid, msg=did)
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

            if action in [rpc.COPY, rpc.LINK, rpc.MOVE]:
                assert tgt.schema == 'file', 'staging tgt expected as file://'

            if action == rpc.TARBALL:

                # If somethig was staged via the tarball method, the tarball is
                # extracted and then removed from the task folder.  The target
                # path is expected to be an *absolute* path on the target system
                # - any relative paths specified by the application are expected
                # to get expanded on the client side.
                tarball = '%s/%s.tar' % (os.path.dirname(tgt.path), uid)
                self._log.debug('extract tarball for %s', tarball)
                tar = tarfile.open(tarball)
                tar.extractall(path='/')
                tar.close()

              # FIXME: make tarball removal dependent on debug settings
              # os.remove(os.path.dirname(tgt.path) + '/' + uid + '.tar')

            else:

                self._stager.handle_staging_directive({'source': src,
                                                       'target': tgt,
                                                       'action': action,
                                                       'flags' : flags})


            self._prof.prof('staging_in_stop', uid=uid, msg=did)

        # all staging is done -- pass on to the scheduler
        self.advance(task, rps.AGENT_SCHEDULING_PENDING, publish=True, push=True)


# ------------------------------------------------------------------------------

