
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import tempfile
import tarfile

import radical.utils as ru

from ...   import states    as rps
from ...   import constants as rpc
from ...   import utils     as rpu

from .base import TMGRStagingInputComponent

from ...staging_directives import complete_url, expand_staging_directives


# if we receive more than a certain numnber of tasks in a bulk, we create the
# task sandboxes in a remote bulk op.  That limit is defined here, along with
# the definition of the bulk mechanism used to create the sandboxes:
#   tar : unpack a locally created tar which contains all sandboxes

TASK_BULK_MKDIR_THRESHOLD = 1024 * 1024
TASK_BULK_MKDIR_MECHANISM = 'tar'


# ------------------------------------------------------------------------------
#
class Default(TMGRStagingInputComponent):
    '''
    This component performs all tmgr side input staging directives for compute
    tasks.  It gets tasks from the tmgr_staging_input_queue, in
    TMGR_STAGING_INPUT_PENDING state, will advance them to TMGR_STAGING_INPUT
    state while performing the staging, and then moves then to the
    AGENT_SCHEDULING_PENDING state, passing control to the agent.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        TMGRStagingInputComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._pilots       = dict()
        self._pilots_lock  = ru.RLock()
        self._connected    = list()  # list of pilot conected by ZMQ
        self._session_sbox = self._reg['cfg.session_sandbox']
        self._stager       = rpu.StagingHelper(self._log)
        self._tar_idx      = 0

        self.register_input(rps.TMGR_STAGING_INPUT_PENDING,
                            rpc.TMGR_STAGING_INPUT_QUEUE, self.work)

        # this queue is inaccessible, needs routing via mongodb
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.PROXY_TASK_QUEUE)

        self._mkdir_threshold = self.cfg.get('task_bulk_mkdir_threshold',
                                             TASK_BULK_MKDIR_THRESHOLD)


    # --------------------------------------------------------------------------
    #
    def control_cb(self, topic, msg):

        # keep track of `add_pilots` commands and updates self._pilots
        # accordingly.

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd == 'add_pilots':

            pilots = arg['pilots']

            with self._pilots_lock:

                for pilot in pilots:
                    pid = pilot['uid']
                    self._log.debug('add pilot %s', pid)

                    if pid not in self._pilots:
                        self._pilots[pid] = pilot

        elif cmd == 'pilot_register':

            pid = arg['pid']
            self._log.debug('register pilot %s', pid)

            if pid not in self._connected:
                self._connected.append(pid)

            # let pilot know that tasks will arive via ZMQ
            self.publish(rpc.CONTROL_PUBSUB, msg={'cmd': 'pilot_register_ok',
                                                  'arg': {'pid': pid}})


    # --------------------------------------------------------------------------
    #
    def _advance_tasks(self, tasks, pid=None, state=None, push=True):

        if not state:
            state = rps.AGENT_STAGING_INPUT_PENDING

        # perform and publish state update
        # push to the proxy queue
        for task in tasks:
            self._log.debug_8('push to proxy: %s', task['uid'])

        self.advance(tasks, state, publish=True, push=push, qname=pid)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance(tasks, rps.TMGR_STAGING_INPUT, publish=True, push=False)

        # we first filter out any tasks which don't need any input staging, and
        # advance them again as a bulk.  We work over the others one by one, and
        # advance them individually, to avoid stalling from slow staging ops.

        session_sbox     = self._session_sbox
        staging_tasks    = dict()  # pid: [tasks]
        no_staging_tasks = dict()  # pid: [tasks]

        for task in tasks:

            pid  = task['pilot']
            if pid not in staging_tasks   : staging_tasks[pid]    = list()
            if pid not in no_staging_tasks: no_staging_tasks[pid] = list()

            # check if we have any staging directives to be enacted in this
            # component
            actionables = list()
            for sd in task['description'].get('input_staging', []):
                if sd['action'] in [rpc.TRANSFER, rpc.TARBALL]:
                    actionables.append(sd)

            if actionables:
                staging_tasks[pid].append([task, actionables])
            else:
                no_staging_tasks[pid].append(task)

        # Optimization: if we obtained a large bulk of tasks, we at this point
        # attempt a bulk mkdir for the task sandboxes, to free the agent of
        # performing that operation.  That implies that the agent needs to check
        # sandbox existence before attempting to create them now.
        #
        # Note that this relies on the tmgr scheduler to assigning the sandbox
        # to the task.
        #
        # Note further that we need to make sure that all tasks are actually
        # pointing into the same target file system, so we need to cluster by
        # filesystem before checking the bulk size.  For simplicity we actually
        # cluster by pilot ID, which is sub-optimal for task bulks which go to
        # different pilots on the same resource (think OSG).
        #
        # Note further that we skip the bulk-op for all tasks for which we
        # actually need to stage data, since the mkdir will then implicitly be
        # done anyways.
        #
        # Caveat: we can actually only (reasonably) do this if we know some
        # details about the pilot, because otherwise we'd have too much guessing
        # to do about the pilot configuration (sandbox, access schema, etc), so
        # we only attempt this optimization for tasks scheduled to pilots for
        # which we learned those details.
        sboxes = dict()  # pid: [sboxes]
        for pid in no_staging_tasks:
            for task in no_staging_tasks[pid]:
                if pid not in sboxes:
                    sboxes[pid] = list()
                sboxes[pid].append(task['task_sandbox'])

        # now trigger the bulk mkdir for all filesystems which have more than
        # a certain tasks tohandle in this bulk:
        for pid in sboxes:

            with self._pilots_lock:
                pilot = self._pilots.get(pid)

            if not pilot:
                # we don't feel inclined to optimize for unknown pilots
                self._log.debug('pid unknown - skip optimization', pid)
                continue

            task_sboxes  = sboxes[pid]

            if len(task_sboxes) >= self._mkdir_threshold:
                self._log.debug('tar %d sboxes', len(task_sboxes))

                session_sbox = self._session._get_session_sandbox(pilot)

                # create a tarball with all task sandboxes, push
                # it over, and untar it (one untar op then creates all dirs).
                if TASK_BULK_MKDIR_MECHANISM == 'tar':

                    tmp_path = tempfile.mkdtemp(prefix='rp_agent_tar_dir')
                    tmp_dir  = os.path.abspath(tmp_path)
                    tar_name = '%s.%s.%04d.tar' % (self._session.uid, self.uid,
                                                   self._tar_idx)
                    tar_tgt  = '%s/%s'         % (tmp_dir, tar_name)
                    tar_url  = ru.Url('file://localhost/%s' % tar_tgt)

                    self._tar_idx += 1

                    # we want pathnames which are relative to the session
                    # sandbox.  Ignore all other sandboxes - the agent will have
                    # to create those.
                    root = str(session_sbox)
                    rlen = len(root)
                    rels = list()
                    for path in task_sboxes:
                        if path.startswith(root):
                            rels.append(path[rlen + 1:])

                    rpu.create_tar(tar_tgt, rels)

                    tar_rem_path = "%s/%s" % (str(session_sbox), tar_name)

                    self._log.debug('sbox: %s [%s]', session_sbox,
                                                             type(session_sbox))
                    self._log.debug('copy: %s -> %s', tar_url, tar_rem_path)

                    self._prof.prof('staging_in_start', uid=pid, msg='tar')
                    self._stager.copy(tar_url, tar_rem_path)
                    self._prof.prof('staging_in_stop', uid=pid, msg='tar')

                    # get a job service handle to the target resource and run
                    # the untar command.  Use the hop to skip the batch system
                    hop_url = pilot['js_hop']
                    self._log.debug('js  : %s', hop_url)

                    cmd = "tar xvf %s/%s -C %s" % (session_sbox.path, tar_name,
                                                   session_sbox.path)

                    out, err, ret = self._stager.sh_callout(hop_url, cmd)
                    self._log.debug('untar: %s', [cmd, out, err, ret])


        for pid in no_staging_tasks:
            if no_staging_tasks[pid]:
                # nothing to stage, push to the agent
                self._advance_tasks(no_staging_tasks[pid], pid)

        to_fail = list()
        for pid in staging_tasks:
            for task, actionables in staging_tasks[pid]:
                try:
                    self._handle_task(task, actionables)
                    self._advance_tasks([task], pid)

                except Exception as e:
                    # staging failed - do not pass task to agent
                    self._log.exception('staging for %s failed', task['uid'])
                    task['control']          = 'tmgr'
                    task['exception']        = repr(e)
                    task['exception_detail'] = '\n'.join(ru.get_exception_trace())
                    to_fail.append(task)

        self._advance_tasks(to_fail, state=rps.FAILED, push=False)


    # --------------------------------------------------------------------------
    #
    def _handle_task(self, task, actionables):

        # FIXME: we should created task sandboxes in a bulk

        uid = task['uid']

        self._prof.prof("create_sandbox_start", uid=uid)

        src_context = {'pwd'      : task['client_sandbox'],     # !!!
                       'client'   : task['client_sandbox'],
                       'task'     : task['task_sandbox'],
                       'pilot'    : task['pilot_sandbox'],
                       'session'  : task['session_sandbox'],
                       'resource' : task['resource_sandbox'],
                       'endpoint' : task['endpoint_fs']}
        tgt_context = {'pwd'      : task['task_sandbox'],       # !!!
                       'client'   : task['client_sandbox'],
                       'task'     : task['task_sandbox'],
                       'pilot'    : task['pilot_sandbox'],
                       'session'  : task['session_sandbox'],
                       'resource' : task['resource_sandbox'],
                       'endpoint' : task['endpoint_fs']}

        # we have actionable staging directives, and thus we need a task
        # sandbox.
        sandbox = ru.Url(task["task_sandbox"])

        self._stager.mkdir(sandbox)
        self._prof.prof("create_sandbox_stop", uid=uid)

        # Loop over all transfer directives and filter out tarball staging
        # directives.  Those files are added into a tarball, and a single
        # actionable to stage that tarball replaces the original actionables.

        # create a new actionable list during the filtering
        new_actionables = list()
        tar_file        = None
        tar_path        = None
        tar_sd          = None

        for sd in actionables:

            # don't touch non-tar SDs
            if sd['action'] != rpc.TARBALL:
                new_actionables.append(sd)

            else:
                did    = sd['uid']
                src    = sd['source']
                tgt    = sd['target']

                src = complete_url(src, src_context, self._log)
                tgt = complete_url(tgt, tgt_context, self._log)

                self._prof.prof('staging_in_tar_start', uid=uid, msg=did)

                # create a tarfile on the first match, and register for transfer
                if not tar_file:
                    tmp_file = tempfile.NamedTemporaryFile(
                                                prefix='rp_usi_%s.' % uid,
                                                suffix='.tar',
                                                delete=False)
                    tar_path = tmp_file.name
                    tar_file = tarfile.open(fileobj=tmp_file, mode='w')
                    tar_src  = ru.Url('file://localhost/%s' % tar_path)
                    tar_tgt  = ru.Url('task:///%s.tar'      % uid)
                    tar_did  = ru.generate_id('sd')
                    tar_sd   = {'action' : rpc.TRANSFER,
                                'flags'  : rpc.DEFAULT_FLAGS,
                                'uid'    : tar_did,
                                'source' : str(tar_src),
                                'target' : str(tar_tgt),
                               }
                    new_actionables.append(tar_sd)

                    self._log.debug('create tar sd %s', tar_sd)

                # add the src file
                tar_file.add(src.path, arcname=tgt.path)

                self._prof.prof('staging_in_tar_stop',  uid=uid, msg=did)


        # make sure tarball is flushed to disk
        if tar_file:
            tar_file.close()

        new_actionables = expand_staging_directives(new_actionables,
                                            src_context, tgt_context, self._log)

        # work on the filtered TRANSFER actionables
        for sd in new_actionables:
            self._prof.prof('staging_in_start', uid=uid, msg=sd['uid'])
            self._stager.handle_staging_directive(sd)
            self._prof.prof('staging_in_stop', uid=uid, msg=sd['uid'])

        if tar_file:

            assert tar_path
            assert tar_sd

            # some tarball staging was done.  Add a staging directive for the
            # agent to untar the tarball, and clean up.
            tar_sd['action'] = rpc.TARBALL
            task['description']['input_staging'].append(tar_sd)
            os.remove(tar_path)


# ------------------------------------------------------------------------------

