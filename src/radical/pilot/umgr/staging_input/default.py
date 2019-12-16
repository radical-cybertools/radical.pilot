
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import tempfile
import tarfile

import radical.utils as ru
import radical.saga  as rs

rs.fs = rs.filesystem

from ...   import states    as rps
from ...   import constants as rpc
from ...   import utils     as rpu

from .base import UMGRStagingInputComponent

from ...staging_directives import complete_url


# if we receive more than a certain numnber of units in a bulk, we create the
# unit sandboxes in a remote bulk op.  That limit is defined here, along with
# the definition of the bulk mechanism used to create the sandboxes:
#   saga: use SAGA bulk ops
#   tar : unpack a locally created tar which contains all sandboxes

UNIT_BULK_MKDIR_THRESHOLD = 16
UNIT_BULK_MKDIR_MECHANISM = 'tar'


# ------------------------------------------------------------------------------
#
class Default(UMGRStagingInputComponent):
    """
    This component performs all umgr side input staging directives for compute
    units.  It gets units from the umgr_staging_input_queue, in
    UMGR_STAGING_INPUT_PENDING state, will advance them to UMGR_STAGING_INPUT
    state while performing the staging, and then moves then to the
    AGENT_SCHEDULING_PENDING state, passing control to the agent.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        UMGRStagingInputComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # we keep a cache of SAGA dir handles
        self._fs_cache    = dict()
        self._js_cache    = dict()
        self._pilots      = dict()
        self._pilots_lock = ru.RLock()

        self.register_input(rps.UMGR_STAGING_INPUT_PENDING,
                            rpc.UMGR_STAGING_INPUT_QUEUE, self.work)

        # FIXME: this queue is inaccessible, needs routing via mongodb
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING, None)

        # we subscribe to the command channel to learn about pilots being added
        # to this unit manager.
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._base_command_cb)



    # --------------------------------------------------------------------------
    #
    def finalize(self):

        [fs.close() for fs in list(self._fs_cache.values())]
        [js.close() for js in list(self._js_cache.values())]


    # --------------------------------------------------------------------------
    #
    def _base_command_cb(self, topic, msg):

        # keep track of `add_pilots` commands and updates self._pilots
        # accordingly.

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd not in ['add_pilots']:
            self._log.debug('skip cmd %s', cmd)

        pilots = arg.get('pilots', [])

        if not isinstance(pilots, list):
            pilots = [pilots]

        with self._pilots_lock:
            for pilot in pilots:
                pid = pilot['uid']
                self._log.debug('add pilot %s', pid)
                if pid not in self._pilots:
                    self._pilots[pid] = pilot

        return True


    # --------------------------------------------------------------------------
    #
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.UMGR_STAGING_INPUT, publish=True, push=False)

        # we first filter out any units which don't need any input staging, and
        # advance them again as a bulk.  We work over the others one by one, and
        # advance them individually, to avoid stalling from slow staging ops.

        no_staging_units = list()
        staging_units    = list()

        for unit in units:

            # no matter if we perform any staging or not, we will push the full
            # unit info to the DB on the next advance, and will pass control to
            # the agent.
            unit['$all']    = True
            unit['control'] = 'agent_pending'

            # check if we have any staging directives to be enacted in this
            # component
            actionables = list()
            for sd in unit['description'].get('input_staging', []):
                if sd['action'] in [rpc.TRANSFER, rpc.TARBALL]:
                    actionables.append(sd)

            if actionables:
                staging_units.append([unit, actionables])
            else:
                no_staging_units.append(unit)

        # Optimization: if we obtained a large bulk of units, we at this point
        # attempt a bulk mkdir for the unit sandboxes, to free the agent of
        # performing that operation.  That implies that the agent needs to check
        # sandbox existence before attempting to create them now.
        #
        # Note that this relies on the umgr scheduler to assigning the sandbox
        # to the unit.
        #
        # Note further that we need to make sure that all units are actually
        # pointing into the same target file system, so we need to cluster by
        # filesystem before checking the bulk size.  For simplicity we actually
        # cluster by pilot ID, which is sub-optimal for unit bulks which go to
        # different pilots on the same resource (think OSG).
        #
        # Note further that we skip the bulk-op for all units for which we
        # actually need to stage data, since the mkdir will then implicitly be
        # done anyways.
        #
        # Caveat: we can actually only (reasonably) do this if we know some
        # details about the pilot, because otherwise we'd have too much guessing
        # to do about the pilot configuration (sandbox, access schema, etc), so
        # we only attempt this optimization for units scheduled to pilots for
        # which we learned those details.
        unit_sboxes_by_pid = dict()
        for unit in no_staging_units:
            sbox = unit['unit_sandbox']
            pid  = unit['pilot']
            if pid not in unit_sboxes_by_pid:
                unit_sboxes_by_pid[pid] = list()
            unit_sboxes_by_pid[pid].append(sbox)

        # now trigger the bulk mkdir for all filesystems which have more than
        # a certain units tohandle in this bulk:
        for pid in unit_sboxes_by_pid:

            with self._pilots_lock:
                pilot = self._pilots.get(pid)

            if not pilot:
                # we don't feel inclined to optimize for unknown pilots
                self._log.debug('pid unknown - skip optimizion', pid)
                continue

            session_sbox = self._session._get_session_sandbox(pilot)
            unit_sboxes  = unit_sboxes_by_pid[pid]

            if len(unit_sboxes) >= UNIT_BULK_MKDIR_THRESHOLD:

                self._log.debug('tar %d sboxes', len(unit_sboxes))

                # no matter the bulk mechanism, we need a SAGA handle to the
                # remote FS
                sbox_fs      = ru.Url(session_sbox)  # deep copy
                sbox_fs.path = '/'
                sbox_fs_str  = str(sbox_fs)
                if sbox_fs_str not in self._fs_cache:
                    self._fs_cache[sbox_fs_str] = \
                            rs.fs.Directory(sbox_fs, session=self._session)
                saga_dir = self._fs_cache[sbox_fs_str]

                # we have two options for a bulk mkdir:
                # 1) ask SAGA to create the sandboxes in a bulk op
                # 2) create a tarball with all unit sandboxes, push
                #    it over, and untar it (one untar op then creates all dirs).
                #    We implement both
                if UNIT_BULK_MKDIR_MECHANISM == 'saga':

                    tc = rs.task.Container()
                    for sbox in unit_sboxes:
                        tc.add(saga_dir.make_dir(sbox, ttype=rs.TASK))
                    tc.run()
                    tc.wait()

                elif UNIT_BULK_MKDIR_MECHANISM == 'tar':

                    tmp_path = tempfile.mkdtemp(prefix='rp_agent_tar_dir')
                    tmp_dir  = os.path.abspath(tmp_path)
                    tar_name = '%s.%s.tar' % (self._session.uid, self.uid)
                    tar_tgt  = '%s/%s'     % (tmp_dir, tar_name)
                    tar_url  = ru.Url('file://localhost/%s' % tar_tgt)

                    # we want pathnames which are relative to the session
                    # sandbox.  Ignore all other sandboxes - the agent will have
                    # to create those.
                    root = str(session_sbox)
                    rlen = len(root)
                    rels = list()
                    for path in unit_sboxes:
                        if path.startswith(root):
                            rels.append(path[rlen + 1:])

                    rpu.create_tar(tar_tgt, rels)

                    tar_rem_path = "%s/%s" % (str(session_sbox), tar_name)

                    self._log.debug('sbox: %s [%s]', session_sbox,
                                                             type(session_sbox))
                    self._log.debug('copy: %s -> %s', tar_url, tar_rem_path)
                    saga_dir.copy(tar_url, tar_rem_path,
                                             flags=rs.fs.CREATE_PARENTS)

                    # get a job service handle to the target resource and run
                    # the untar command.  Use the hop to skip the batch system
                    js_url = pilot['js_hop']
                    self._log.debug('js  : %s', js_url)

                    if  js_url in self._js_cache:
                        js_tmp = self._js_cache[js_url]
                    else:
                        js_tmp = rs.job.Service(js_url, session=self._session)
                        self._js_cache[js_url] = js_tmp

                    cmd = "tar xvf %s/%s -C %s" % (session_sbox.path, tar_name,
                                                   session_sbox.path)
                    j   = js_tmp.run_job(cmd)
                    j.wait()
                    self._log.debug('untar : %s', cmd)
                    self._log.debug('untar : %s\n---\n%s\n---\n%s',
                            j.get_stdout_string(), j.get_stderr_string(),
                            j.exit_code)


        if no_staging_units:

            # nothing to stage, push to the agent
            self.advance(no_staging_units, rps.AGENT_STAGING_INPUT_PENDING,
                         publish=True, push=True)

        for unit,actionables in staging_units:
            self._handle_unit(unit, actionables)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, unit, actionables):

        # FIXME: we should created unit sandboxes in a bulk

        uid = unit['uid']

        self._prof.prof("create_sandbox_start", uid=uid)

        src_context = {'pwd'      : os.getcwd(),                # !!!
                       'unit'     : unit['unit_sandbox'],
                       'pilot'    : unit['pilot_sandbox'],
                       'resource' : unit['resource_sandbox']}
        tgt_context = {'pwd'      : unit['unit_sandbox'],       # !!!
                       'unit'     : unit['unit_sandbox'],
                       'pilot'    : unit['pilot_sandbox'],
                       'resource' : unit['resource_sandbox']}

        # we have actionable staging directives, and thus we need a unit
        # sandbox.
        sandbox = rs.Url(unit["unit_sandbox"])
        tmp     = rs.Url(unit["unit_sandbox"])

        # url used for cache (sandbox url w/o path)
        tmp.path = '/'
        key = str(tmp)
        self._log.debug('key %s / %s', key, tmp)

        if key not in self._fs_cache:
            self._fs_cache[key] = rs.fs.Directory(tmp, session=self._session)

        saga_dir = self._fs_cache[key]
        saga_dir.make_dir(sandbox, flags=rs.fs.CREATE_PARENTS)
        self._prof.prof("create_sandbox_stop", uid=uid)

        # Loop over all transfer directives and filter out tarball staging
        # directives.  Those files are added into a tarball, and a single
        # actionable to stage that tarball replaces the original actionables.

        # create a new actionable list during the filtering
        new_actionables = list()
        tar_file        = None

        for sd in actionables:

            # don't touch non-tar SDs
            if sd['action'] != rpc.TARBALL:
                new_actionables.append(sd)

            else:

                action = sd['action']
                flags  = sd['flags']   # NOTE: we don't use those
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
                    tar_tgt  = ru.Url('unit:////%s.tar'     % uid)
                    tar_did  = ru.generate_id('sd')
                    tar_sd   = {'action' : rpc.TRANSFER,
                                'flags'  : rpc.DEFAULT_FLAGS,
                                'uid'    : tar_did,
                                'source' : str(tar_src),
                                'target' : str(tar_tgt),
                               }
                    new_actionables.append(tar_sd)

                # add the src file
                tar_file.add(src.path, arcname=tgt.path)

                self._prof.prof('staging_in_tar_stop',  uid=uid, msg=did)


        # make sure tarball is flushed to disk
        if tar_file:
            tar_file.close()

        # work on the filtered TRANSFER actionables
        for sd in new_actionables:

            action = sd['action']
            flags  = sd['flags']
            did    = sd['uid']
            src    = sd['source']
            tgt    = sd['target']

            if action == rpc.TRANSFER:

                src = complete_url(src, src_context, self._log)
                tgt = complete_url(tgt, tgt_context, self._log)

                # Check if the src is a folder, if true
                # add recursive flag if not already specified
                if os.path.isdir(src.path):
                    flags |= rs.fs.RECURSIVE

                # Always set CREATE_PARENTS
                flags |= rs.fs.CREATE_PARENTS

                src = complete_url(src, src_context, self._log)
                tgt = complete_url(tgt, tgt_context, self._log)

                self._prof.prof('staging_in_start', uid=uid, msg=did)
                saga_dir.copy(src, tgt, flags=flags)
                self._prof.prof('staging_in_stop', uid=uid, msg=did)


        if tar_file:

            # some tarball staging was done.  Add a staging directive for the
            # agent to untar the tarball, and clean up.
            tar_sd['action'] = rpc.TARBALL
            unit['description']['input_staging'].append(tar_sd)
            os.remove(tar_path)


        # staging is done, we can advance the unit at last
        self.advance(unit, rps.AGENT_STAGING_INPUT_PENDING,
                           publish=True, push=True)


# ------------------------------------------------------------------------------

