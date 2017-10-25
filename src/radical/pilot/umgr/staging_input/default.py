
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import tempfile
import multithreading      as mt

import saga                as rs
import radical.utils       as ru

from .... import pilot     as rp
from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import UMGRStagingInputComponent

from ...staging_directives import complete_url


# if we receive more than a certain numnber of units in a bulk, we create the
# unit sandboxes in a remote bulk op.  That limit is defined here, along with
# the definition of the bulk mechanism used to create the sandboxes:
#   saga: use SAGA bulk ops
#   tar : unpack a locally created tar which contains all sandboxes

UNIT_BULK_MKDIR_THRESHOLD = 128
UNIT_BULK_MKDIR_MECHANISM = 'saga'


# ==============================================================================
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
    def initialize_child(self):

        # we keep a cache of SAGA dir handles
        self._fs_cache    = dict()
        self._js_cache    = dict()
        self._pilots      = dict()
        self._pilots_lock = mt.RLock()

        self.register_input(rps.UMGR_STAGING_INPUT_PENDING,
                            rpc.UMGR_STAGING_INPUT_QUEUE, self.work)

        # FIXME: this queue is inaccessible, needs routing via mongodb
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING, None)

        # we subscribe to pilot state updates, as we learn about pilot
        # configurations this way, which we can use for some optimizations.
        self.register_subscriber(rpc.STATE_PUBSUB, self._base_state_cb)


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        self.unregister_subscriber(rpc.STATE_PUBSUB, self._base_state_cb)

        try:
            [fs.close() for fs in self._fs_cache.values()]
            [js.close() for js in self._js_cache.values()]

        except:
            pass


    # --------------------------------------------------------------------------
    #
    def _base_state_cb(self, topic, msg):

        # keep track of pilot state changes and updates self._pilots
        # accordingly.  Unit state changes are be ignored.

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        # FIXME: get cmd string consistent throughout the code
        if cmd not in ['update', 'state_update']:
            return True

        if not isinstance(arg, list): things = [arg]
        else                        : things =  arg

        pilots = [t for t in things if t['type'] == 'pilot']

        for pilot in pilots:
            pid = pilot['uid']
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

                if sd['action'] == rpc.TRANSFER:
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
        # details about the pilot, because otherwise we'd have to much guessing
        # to do about the pilot configuration (sandbox, access schema, etc), so
        # we only attempt this optimization for units scheduled to pilots for
        # which we learned those details.
        units_by_pid = dict()
        for unit in no_staging_units:
            sbox = unit['unit_sandbox']
            pid  = unit['pilot']
            if pid not in units_by_pid:
                units_by_pid[pid] = list()
            units_by_pid[pid].append(sbox)

        # now trigger the bulk mkdiur for all filesystems which have more than
        # a certain units tohandle in this bulk:

        for pid in units_by_pid:

            with self._pilots_lock:
                pilot = self._pilots.get(pid)

            if not pilot:
                # we don't feel inclined to optimize for unknown pilots
                continue
            
            session_sbox = self._session._get_session_sandbox(pilot)
            pilot_sbox   = self._session._get_pilot_sandbox  (pilot)
            unit_sboxes     = units_by_pid[pid]

            if len(unit_sboxes) >= UNIT_BULK_MKDIR_THRESHOLD:

                # no matter the bulk mechanism, we need a SAGA handle to the
                # remote FS
                sbox_fs      = ru.Url(session_sbox)
                sbox_fs.path = '/'
                sbox_fs_str  = str(sbox_fs)
                if sbox_fs_str not in self._fs_cache:
                    self._fs_cache[sbox_fs_str] = rs.filesystem.Directory(sbox_fs,
                                                  session=self._session)
                saga_dir = self._fs_cache[sbox_fs_str]

                # we have two options for a bulk mkdir:
                # 1) ask SAGA to create the sandboxes in a bulk op
                # 2) create a tarball with all unit sandboxes, push it over, and
                #    untar it (one untar op then creates all dirs).  We implement
                #    both
                if UNIT_BULK_MKDIR_MECHANISM == 'saga':

                    tc = rs.task.Container()
                    for sbox in unit_sboxes:
                        tc.add(saga_dir.make_dir(sbox, ttype=rs.TASK))
                    tc.run()
                    tc.wait()

                elif UNIT_BULK_MKDIR_MECHANISM == 'tar':

                    tmp_path = tempfile.mkdtemp(prefix='rp_agent_tar_dir')
                    tmp_dir  = os.path.abspath(tmp_path)
                    tar_name = '%s.%s.tgz' % (self.uid)
                    tar_tgt  = '%s/%s'     % (tmp_dir, tar_name)
                    tar_url  = ru.Url('file://localhost/%s' % tar_tgt)

                    for sbox in unit_sboxes:
                        os.makedirs('%s/%s' % (tmp_dir, sbox))

                    cmd = "cd %s && tar zchf %s *" % (tmp_dir, tar_tgt)
                    out, err, ret = ru.sh_callout(cmd, shell=True)

                    if ret:
                        raise RuntimeError('failed callout %s: %s' % (cmd, err))

                    tar_rem      = rs.Url(fs_url)
                    tar_rem.path = "%s/%s" % (session_sbox, tar_name)

                    saga_dir.copy(tar_url, tar_rem, flags=rs.filesystem.CREATE_PARENTS)

                    ru.sh_callout('rm -r %s' % tmp_path)

                    # get a job service handle to the target resource and run
                    # the untar command
                    resrc   = pilot['description']['resource']
                    schema  = pilot['description']['access_schema']
                    rcfg    = self._session.get_resource_config(resrc, schema)
                    js_ep   = rcfg['job_manager_endpoint']
                    js_hop  = rcfg.get('job_manager_hop', js_ep)
                    js_url  = rs.Url(js_hop)

                    if  js_url in self._js_cache:
                        js_tmp = self._js_cache[js_url]
                    else:
                        js_tmp = rs.job.Service(js_url, session=self._session)
                        self._js_cache[js_url] = js_tmp

                    cmd = "tar zmxvf %s/%s -C %s" % \
                            (session_sbox, tar_name, session_sbox)
                    j = js_tmp.run_job(cmd)
                    j.wait()


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
            self._fs_cache[key] = rs.filesystem.Directory(tmp, 
                                             session=self._session)

        saga_dir = self._fs_cache[key]
        saga_dir.make_dir(sandbox, flags=rs.filesystem.CREATE_PARENTS)
        self._prof.prof("create_sandbox_stop", uid=uid)

        # Loop over all transfer directives and execute them.
        for sd in actionables:

            action = sd['action']
            flags  = sd['flags']
            did    = sd['uid']
            src    = sd['source']
            tgt    = sd['target']

            self._prof.prof('staging_in_start', uid=uid, msg=did)

            src = complete_url(src, src_context, self._log)
            tgt = complete_url(tgt, tgt_context, self._log)

            if rpc.CREATE_PARENTS in flags:
                copy_flags = rs.filesystem.CREATE_PARENTS
            else:
                copy_flags = 0

            saga_dir.copy(src, tgt, flags=copy_flags)
            self._prof.prof('staging_in_stop', uid=uid, msg=did)

        # staging is done, we can advance the unit at last
        self.advance(unit, rps.AGENT_STAGING_INPUT_PENDING, publish=True, push=True)


# ------------------------------------------------------------------------------

