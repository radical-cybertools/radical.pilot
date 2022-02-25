# pylint: disable=protected-access

__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import math
import time
import pprint
import shutil
import tempfile
import threading as mt

import radical.gtod            as rg
import radical.utils           as ru
import radical.saga            as rs

from ...  import states        as rps
from ...  import constants     as rpc

from .base import PMGRLaunchingComponent

from ...staging_directives import complete_url, expand_staging_directives


# ------------------------------------------------------------------------------
# local constants
DEFAULT_AGENT_SPAWNER = 'POPEN'
DEFAULT_RP_VERSION    = 'local'
DEFAULT_VIRTENV_MODE  = 'update'
DEFAULT_VIRTENV_DIST  = 'default'
DEFAULT_AGENT_CONFIG  = 'default'

JOB_CANCEL_DELAY      = 120  # seconds between cancel signal and job kill
JOB_CHECK_INTERVAL    =  60  # seconds between runs of the job state check loop
JOB_CHECK_MAX_MISSES  =   3  # number of times to find a job missing before
                             # declaring it dead

LOCAL_SCHEME   = 'file'


# ------------------------------------------------------------------------------
#
class Default(PMGRLaunchingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        PMGRLaunchingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # we don't really have an output queue, as we pass control over the
        # pilot jobs to the resource management system (ResourceManager).

        self._pilots        = dict()      # dict for all known pilots
        self._pilots_lock   = mt.RLock()  # lock on maipulating the above
        self._checking      = list()      # pilots to check state on
        self._check_lock    = mt.RLock()  # lock on maipulating the above
        self._saga_js_cache = dict()      # cache of saga job services
        self._sandboxes     = dict()      # cache of resource sandbox URLs
        self._cache_lock    = mt.RLock()  # lock for cache

        self._mod_dir       = os.path.dirname(os.path.abspath(__file__))
        self._root_dir      = "%s/../../"   % self._mod_dir

        self.register_input(rps.PMGR_LAUNCHING_PENDING,
                            rpc.PMGR_LAUNCHING_QUEUE, self.work)

        self._stager_queue = self.get_output_ep(rpc.STAGER_REQUEST_QUEUE)

        # FIXME: make interval configurable
        self.register_timed_cb(self._pilot_watcher_cb, timer=10.0)

        # we listen for pilot cancel commands
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._pmgr_control_cb)

        # also listen for completed staging directives
        self.register_subscriber(rpc.STAGER_RESPONSE_PUBSUB, self._staging_ack_cb)
        self._active_sds = dict()
        self._sds_lock   = mt.Lock()


        self._log.info(ru.get_version([self._mod_dir, self._root_dir]))
        self._rp_version, _, _, _, self._rp_sdist_name, self._rp_sdist_path = \
                ru.get_version([self._mod_dir, self._root_dir])


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        try:
            self.unregister_timed_cb(self._pilot_watcher_cb)
            self.unregister_input(rps.PMGR_LAUNCHING_PENDING,
                                  rpc.PMGR_LAUNCHING_QUEUE, self.work)

            # FIXME: always kill all saga jobs for non-final pilots at termination,
            #        and set the pilot states to CANCELED.  This will conflict with
            #        disconnect/reconnect semantics.
            with self._pilots_lock:
                pids = list(self._pilots.keys())

            self._cancel_pilots(pids)
            self._kill_pilots(pids)

            with self._cache_lock:
                for url,js in self._saga_js_cache.items():
                    self._log.debug('close js %s', url)
                    js.close()

        except:
            self._log.exception('finalization error')


    # --------------------------------------------------------------------------
    #
    def _pmgr_control_cb(self, topic, msg):

        cmd = msg['cmd']
        arg = msg['arg']

        self._log.debug('launcher got %s', msg)

        if cmd == 'cancel_pilots':

            # on cancel_pilot requests, we forward the DB entries via MongoDB,
            # by pushing a pilot update.  We also mark the pilot for
            # cancelation, so that the pilot watcher can cancel the job after
            # JOB_CANCEL_DELAY seconds, in case the pilot did not react on the
            # command in time.

            pmgr = arg['pmgr']
            pids = arg['uids']

            if pmgr != self._pmgr:
                # this request is not for us to enact
                return True

            if not isinstance(pids, list):
                pids = [pids]

            self._log.info('received pilot_cancel command (%s)', pids)

            self._cancel_pilots(pids)


        return True


    # --------------------------------------------------------------------------
    #
    def _pilot_watcher_cb(self):

        # FIXME: we should actually use SAGA job state notifications!
        # FIXME: check how race conditions are handles: we may detect
        #        a finalized SAGA job and change the pilot state -- but that
        #        pilot may have transitioned into final state via the normal
        #        notification mechanism already.  That probably should be sorted
        #        out by the pilot manager, which will receive notifications for
        #        both transitions.  As long as the final state is the same,
        #        there should be no problem anyway.  If it differs, the
        #        'cleaner' final state should prevail, in this ordering:
        #          cancel
        #          timeout
        #          error
        #          disappeared
        #        This implies that we want to communicate 'final_cause'

        # we don't want to lock our members all the time.  For that reason we
        # use a copy of the pilots_tocheck list and iterate over that, and only
        # lock other members when they are manipulated.
        tc = rs.job.Container()
        with self._pilots_lock, self._check_lock:

            for pid in self._checking:
                tc.add(self._pilots[pid]['job'])

        states = tc.get_states()

        self._log.debug('bulk states: %s', states)

        # if none of the states is final, we have nothing to do.
        # We can't rely on the ordering of tasks and states in the task
        # container, so we hope that the task container's bulk state query lead
        # to a caching of state information, and we thus have cache hits when
        # querying the pilots individually

        final_pilots = list()
        with self._pilots_lock, self._check_lock:

            for pid in self._checking:

                state = self._pilots[pid]['job'].state
                self._log.debug('saga job state: %s %s %s', pid, self._pilots[pid]['job'],  state)

                if state in [rs.job.DONE, rs.job.FAILED, rs.job.CANCELED]:
                    pilot = self._pilots[pid]['pilot']
                    if state == rs.job.DONE    : pilot['state'] = rps.DONE
                    if state == rs.job.FAILED  : pilot['state'] = rps.FAILED
                    if state == rs.job.CANCELED: pilot['state'] = rps.CANCELED
                    final_pilots.append(pilot)

        if final_pilots:

            for pilot in final_pilots:

                with self._check_lock:
                    # stop monitoring this pilot
                    self._checking.remove(pilot['uid'])

                self._log.debug('final pilot %s %s', pilot['uid'], pilot['state'])

            self.advance(final_pilots, push=False, publish=True)

        # all checks are done, final pilots are weeded out.  Now check if any
        # pilot is scheduled for cancellation and is overdue, and kill it
        # forcefully.
        to_cancel  = list()
        with self._pilots_lock:

            for pid in self._pilots:

                pilot   = self._pilots[pid]['pilot']
                time_cr = pilot.get('cancel_requested')

                # check if the pilot is final meanwhile
                if pilot['state'] in rps.FINAL:
                    continue

                if time_cr and time_cr + JOB_CANCEL_DELAY < time.time():
                    self._log.debug('pilot needs killing: %s :  %s + %s < %s',
                            pid, time_cr, JOB_CANCEL_DELAY, time.time())
                    del(pilot['cancel_requested'])
                    self._log.debug(' cancel pilot %s', pid)
                    to_cancel.append(pid)

        if to_cancel:
            self._kill_pilots(to_cancel)

        return True


    # --------------------------------------------------------------------------
    #
    def _cancel_pilots(self, pids):
        '''
        Send a cancellation request to the pilots.  This call will not wait for
        the request to get enacted, nor for it to arrive, but just send it.
        '''

        if not pids or not self._pilots:
            # nothing to do
            return

        # recod time of request, so that forceful termination can happen
        # after a certain delay
        now = time.time()
        with self._pilots_lock:
            for pid in pids:
                if pid in self._pilots:
                    self._log.debug('update cancel req: %s %s', pid, now)
                    self._pilots[pid]['pilot']['cancel_requested'] = now


    # --------------------------------------------------------------------------
    #
    def _kill_pilots(self, pids):
        '''
        Forcefully kill a set of pilots.  For pilots which have just recently be
        cancelled, we will wait a certain amount of time to give them a chance
        to termimate on their own (which allows to flush profiles and logfiles,
        etc).  After that delay, we'll make sure they get killed.
        '''

        self._log.debug('killing pilots: %s', pids)

        if not pids or not self._pilots:
            # nothing to do
            return

        # find the most recent cancellation request
        with self._pilots_lock:
            self._log.debug('killing pilots: %s',
                              [p['pilot'].get('cancel_requested', 0)
                               for p in list(self._pilots.values())])
            last_cancel = max([p['pilot'].get('cancel_requested', 0)
                               for p in list(self._pilots.values())])

        self._log.debug('killing pilots: last cancel: %s', last_cancel)

        # we wait for up to JOB_CANCEL_DELAY for a pilt
        while time.time() < (last_cancel + JOB_CANCEL_DELAY):

            self._log.debug('killing pilots: check %s < %s + %s',
                    time.time(), last_cancel, JOB_CANCEL_DELAY)

            alive_pids = list()
            for pid in pids:

                if pid not in self._pilots:
                    self._log.error('unknown: %s', pid)
                    raise ValueError('unknown pilot %s' % pid)

                pilot = self._pilots[pid]['pilot']
                if pilot['state'] not in rps.FINAL:
                    self._log.debug('killing pilots: alive %s', pid)
                    alive_pids.append(pid)
                else:
                    self._log.debug('killing pilots: dead  %s', pid)

            pids = alive_pids
            if not alive_pids:
                # nothing to do anymore
                return

            # avoid busy poll)
            time.sleep(1)

        to_advance = list()

        # we don't want the watcher checking for these pilot anymore
        with self._check_lock:
            for pid in pids:
                if pid in self._checking:
                    self._checking.remove(pid)


        self._log.debug('killing pilots: kill! %s', pids)
        try:
            with self._pilots_lock:
                tc = rs.job.Container()
                for pid in pids:

                    if pid not in self._pilots:
                        self._log.error('unknown: %s', pid)
                        raise ValueError('unknown pilot %s' % pid)

                    pilot = self._pilots[pid]['pilot']
                    job   = self._pilots[pid]['job']

                    # don't overwrite resource_details from the agent
                    if 'resource_details' in pilot:
                        del(pilot['resource_details'])

                    if pilot['state'] in rps.FINAL:
                        continue

                    self._log.debug('plan cancellation of %s : %s', pilot, job)
                    to_advance.append(pilot)
                    self._log.debug('request cancel for %s', pilot['uid'])
                    tc.add(job)

                self._log.debug('cancellation start')
                tc.cancel()
                tc.wait()
                self._log.debug('cancellation done')

            # set canceled state
            self.advance(to_advance, state=rps.CANCELED, push=False, publish=True)

        except Exception:
            self._log.exception('pilot kill failed')

        return True


    # --------------------------------------------------------------------------
    #
    def work(self, pilots):

        if not isinstance(pilots, list):
            pilots = [pilots]

        self.advance(pilots, rps.PMGR_LAUNCHING, publish=True, push=False)

        # We can only use bulk submission for pilots which go to the same
        # target, thus we sort them into buckets and lunch the buckets
        # individually
        buckets = dict()
        for pilot in pilots:
            resource = pilot['description']['resource']
            schema   = pilot['description']['access_schema']
            if resource not in buckets:
                buckets[resource] = dict()
            if schema not in buckets[resource]:
                buckets[resource][schema] = list()
            buckets[resource][schema].append(pilot)

        for resource in buckets:

            for schema in buckets[resource]:

                try:
                    pilots = buckets[resource][schema]
                    pids   = [p['uid'] for p in pilots]
                    self._log.info("Launching pilots on %s: %s", resource, pids)

                    self._start_pilot_bulk(resource, schema, pilots)

                    self.advance(pilots, rps.PMGR_ACTIVE_PENDING,
                                         push=False, publish=True)

                except Exception:
                    self._log.exception('bulk launch failed')
                    self.advance(pilots, rps.FAILED, push=False, publish=True)


    # --------------------------------------------------------------------------
    #
    def _start_pilot_bulk(self, resource, schema, pilots):
        '''
        For each pilot, we prepare by determining what files need to be staged,
        and what job description needs to be submitted.  Files are then be
        staged, and jobs are submitted.

        Two files are staged: a bootstrapper and a tarball - the latter
        containing the pilot sandboxes, agent configs, and any other auxilliary
        files needed to bootstrap.  The bootstrapper will untar those parts of
        the tarball which it needs to bootstrap one specific pilot.
        '''

        rcfg = self._session.get_resource_config(resource, schema)
        sid  = self._session.uid

        # ----------------------------------------------------------------------
        # the rcfg can contain keys with string expansion placeholders where
        # values from the pilot description need filling in.  A prominent
        # example is `%(pd.project)s`, where the pilot description's `PROJECT`
        # value needs to be filled in (here in lowercase).
        #
        # FIXME: right now we assume all pilot descriptions to contain similar
        #        entries, so that the expansion is only done on the first PD.
        expand = dict()
        pd     = pilots[0]['description']
        for k,v in pd.items():
            if v is None:
                v = ''
            expand['pd.%s' % k] = v
            if isinstance(v, str):
                expand['pd.%s' % k.upper()] = v.upper()
                expand['pd.%s' % k.lower()] = v.lower()
            else:
                expand['pd.%s' % k.upper()] = v
                expand['pd.%s' % k.lower()] = v

        for k in rcfg:
            if isinstance(rcfg[k], str):
                orig     = rcfg[k]
                rcfg[k]  = rcfg[k] % expand
                expanded = rcfg[k]
                if orig != expanded:
                    self._log.debug('RCFG:\n%s\n%s', orig, expanded)

        # we create a fake session_sandbox with all pilot_sandboxes in /tmp, and
        # then tar it up.  Once we untar that tarball on the target machine, we
        # should have all sandboxes and all files required to bootstrap the
        # pilots
        tmp_dir  = os.path.abspath(tempfile.mkdtemp(prefix='rp_agent_tmp'))
        tar_name = '%s.%s.tgz' % (sid, self._uid)
        tar_tgt  = '%s/%s'     % (tmp_dir, tar_name)
        tar_url  = rs.Url('file://localhost/%s' % tar_tgt)

        # we need the session sandbox url, but that is (at least in principle)
        # dependent on the schema to use for pilot startup.  So we confirm here
        # that the bulk is consistent wrt. to the schema.  Also include
        # `staging_input` files and place them in the `pilot_sandbox`.
        #
        # FIXME: if it is not, it needs to be splitted into schema-specific
        # sub-bulks
        #
        schema = pd.get('access_schema')
        for pilot in pilots[1:]:
            assert(schema == pilot['description'].get('access_schema')), \
                    'inconsistent scheme on launch / staging'

        # get and expand sandboxes (this bulk uses the same schema toward the
        # same target resource, so all session sandboxes are the same)
        # FIXME: expansion actually may differ per pilot (queue names, project
        #        names, etc could be expanded)
        session_sandbox = self._session._get_session_sandbox(pilots[0]).path
        session_sandbox = session_sandbox % expand

        # we will create the session sandbox before we untar, so we can use that
        # as workdir, and pack all paths relative to that session sandbox.  That
        # implies that we have to recheck that all URLs in fact do point into
        # the session sandbox.
        #
        # We also create a file `staging_output.json` for each pilot which
        # contains the list of files to be tarred up and prepared for output
        # staging.

        ft_list = list()  # files to stage
        jd_list = list()  # jobs  to submit

        for pilot in pilots:

            pid = pilot['uid']
            os.makedirs('%s/%s' % (tmp_dir, pid))

            info = self._prepare_pilot(resource, rcfg, pilot, expand, tar_name)

            ft_list += info['fts']
            jd_list.append(info['jd'])

            self._prof.prof('staging_in_start', uid=pid)

            for fname in ru.as_list(pilot['description'].get('input_staging')):
                base = os.path.basename(fname)
                ft_list.append({'src': fname,
                                'tgt': '%s/%s' % (pid, base),
                                'rem': False})

            output_staging = pilot['description'].get('output_staging')
            if output_staging:
                fname = '%s/%s/staging_output.txt' % (tmp_dir, pilot['uid'])
                with ru.ru_open(fname, 'w') as fout:
                    for entry in output_staging:
                        fout.write('%s\n' % entry)

            # direct staging, use first pilot for staging context
            # NOTE: this implies that the SDS can only refer to session
            #       sandboxes, not to pilot sandboxes!
            self._stage_in(pilots[0], info['sds'])

        for ft in ft_list:
            src     = os.path.abspath(ft['src'])
            tgt     = os.path.relpath(os.path.normpath(ft['tgt']), session_sandbox)
          # src_dir = os.path.dirname(src)
            tgt_dir = os.path.dirname(tgt)

            if tgt_dir.startswith('..'):
              # raise ValueError('staging tgt %s outside pilot sbox: %s' % (ft['tgt'], tgt))
                tgt = ft['tgt']
                tgt_dir = os.path.dirname(tgt)

            if not os.path.isdir('%s/%s' % (tmp_dir, tgt_dir)):
                os.makedirs('%s/%s' % (tmp_dir, tgt_dir))

            if src == '/dev/null':
                # we want an empty file -- touch it (tar will refuse to
                # handle a symlink to /dev/null)
                ru.ru_open('%s/%s' % (tmp_dir, tgt), 'a').close()
            else:
                # use a shell callout to account for wildcard expansion
                cmd = 'ln -s %s %s/%s' % (os.path.abspath(src), tmp_dir, tgt)
                out, err, ret = ru.sh_callout(cmd, shell=True)
                if ret:
                    self._log.debug('out: %s', out)
                    self._log.debug('err: %s', err)
                    raise RuntimeError('callout failed: %s' % cmd)


        # tar.  If any command fails, this will raise.
        cmd = "cd %s && tar zchf %s *" % (tmp_dir, tar_tgt)
        out, err, ret = ru.sh_callout(cmd, shell=True)

        if ret:
            self._log.debug('out: %s', out)
            self._log.debug('err: %s', err)
            raise RuntimeError('callout failed: %s' % cmd)

        # remove all files marked for removal-after-pack
        for ft in ft_list:
            if ft['rem']:
                os.unlink(ft['src'])

        fs_endpoint  = rcfg['filesystem_endpoint']
        fs_url       = rs.Url(fs_endpoint)
        tar_rem      = rs.Url(fs_url)
        tar_rem.path = "%s/%s" % (session_sandbox, tar_name)

        self._log.debug('stage tarball for %s', pilots[0]['uid'])
        self._stage_in(pilots[0], {'source': tar_url,
                                   'target': tar_rem,
                                   'action': rpc.TRANSFER})
        shutil.rmtree(tmp_dir)

        # FIXME: the untar was moved into the bootstrapper (see `-z`).  That
        #        is actually only correct for the single-pilot case...

        for pilot in pilots:
            self._prof.prof('staging_in_stop',  uid=pilot['uid'])
            self._prof.prof('submission_start', uid=pilot['uid'])

        # look up or create JS for actual pilot submission.  This might result
        # in the same js url as above, or not.
        js_ep  = rcfg['job_manager_endpoint']
        with self._cache_lock:
            if js_ep in self._saga_js_cache:
                js = self._saga_js_cache[js_ep]
            else:
                js = rs.job.Service(js_ep, session=self._session)
                self._saga_js_cache[js_ep] = js

        # now that the scripts are in place and configured,
        # we can launch the agent
        jc = rs.job.Container()

        for jd in jd_list:
            self._log.debug('jd: %s', pprint.pformat(jd.as_dict()))
            jc.add(js.create_job(jd))

        jc.run()

        # Order of tasks in `rs.job.Container().tasks` is not changing over the
        # time, thus it's able to iterate over it and other list(s) all together
        for j, pilot in zip(jc.get_tasks(), pilots):

            # do a quick error check
            if j.state == rs.FAILED:
                self._log.error('%s: %s : %s : %s', j.id, j.state, j.stderr, j.stdout)
                raise RuntimeError("SAGA Job state is FAILED. (%s)" % j.name)

            pid = pilot['uid']

            # Update the Pilot's state to 'PMGR_ACTIVE_PENDING' if SAGA job
            # submission was successful.  Since the pilot leaves the scope of
            # the PMGR for the time being, we update the complete DB document
            pilot['$all'] = True

            # FIXME: update the right pilot
            with self._pilots_lock:

                self._pilots[pid] = dict()
                self._pilots[pid]['pilot'] = pilot
                self._pilots[pid]['job']   = j

            # make sure we watch that pilot
            with self._check_lock:
                self._checking.append(pid)

        for pilot in pilots:
            self._prof.prof('submission_stop', uid=pilot['uid'])


    # --------------------------------------------------------------------------
    #
    def _prepare_pilot(self, resource, rcfg, pilot, expand, tar_name):

        pid = pilot["uid"]
        ret = {'fts': list(),  # tar for staging
               'sds': list(),  # direct staging
               'jd' : None  }  # job description

        # ----------------------------------------------------------------------
        # Database connection parameters
        sid          = self._session.uid
        database_url = self._session.cfg.dburl

        # some default values are determined at runtime
        default_virtenv = '%%(resource_sandbox)s/ve.%s.%s' % \
                          (resource, self._rp_version)

        # ----------------------------------------------------------------------
        # pilot description and resource configuration
        number_cores    = pilot['description']['cores']
        number_gpus     = pilot['description']['gpus']
        required_memory = pilot['description']['memory']
        runtime         = pilot['description']['runtime']
        app_comm        = pilot['description']['app_comm']
        queue           = pilot['description']['queue']
        job_name        = pilot['description']['job_name']
        project         = pilot['description']['project']
        cleanup         = pilot['description']['cleanup']
        candidate_hosts = pilot['description']['candidate_hosts']
        services        = pilot['description']['services']

        # ----------------------------------------------------------------------
        # get parameters from resource cfg, set defaults where needed
        agent_dburl             = rcfg.get('agent_mongodb_endpoint', database_url)
        agent_spawner           = rcfg.get('agent_spawner',       DEFAULT_AGENT_SPAWNER)
        agent_config            = rcfg.get('agent_config',        DEFAULT_AGENT_CONFIG)
        agent_scheduler         = rcfg.get('agent_scheduler')
        tunnel_bind_device      = rcfg.get('tunnel_bind_device')
        default_queue           = rcfg.get('default_queue')
        forward_tunnel_endpoint = rcfg.get('forward_tunnel_endpoint')
        resource_manager        = rcfg.get('resource_manager')
        pre_bootstrap_0         = rcfg.get('pre_bootstrap_0', [])
        pre_bootstrap_1         = rcfg.get('pre_bootstrap_1', [])
        python_interpreter      = rcfg.get('python_interpreter')
        rp_version              = rcfg.get('rp_version')
        virtenv_mode            = rcfg.get('virtenv_mode',        DEFAULT_VIRTENV_MODE)
        virtenv                 = rcfg.get('virtenv',             default_virtenv)
        cores_per_node          = rcfg.get('cores_per_node', 0)
        gpus_per_node           = rcfg.get('gpus_per_node',  0)
        lfs_path_per_node       = rcfg.get('lfs_path_per_node')
        lfs_size_per_node       = rcfg.get('lfs_size_per_node', 0)
        python_dist             = rcfg.get('python_dist')
        virtenv_dist            = rcfg.get('virtenv_dist',        DEFAULT_VIRTENV_DIST)
        task_tmp                = rcfg.get('task_tmp')
        spmd_variation          = rcfg.get('spmd_variation')
        shared_filesystem       = rcfg.get('shared_filesystem', True)
        stage_cacerts           = rcfg.get('stage_cacerts', False)
        task_pre_launch         = rcfg.get('task_pre_launch')
        task_pre_exec           = rcfg.get('task_pre_exec')
        task_pre_rank           = rcfg.get('task_pre_rank')
        task_post_launch        = rcfg.get('task_post_launch')
        task_post_exec          = rcfg.get('task_post_exec')
        task_post_rank          = rcfg.get('task_post_rank')
        mandatory_args          = rcfg.get('mandatory_args', [])
        system_architecture     = rcfg.get('system_architecture', {})
        saga_jd_supplement      = rcfg.get('saga_jd_supplement', {})
        services               += rcfg.get('services', [])

        self._log.debug(cores_per_node)
        self._log.debug(pprint.pformat(rcfg))

        # make sure that mandatory args are known
        for ma in mandatory_args:
            if pilot['description'].get(ma) is None:
                raise  ValueError('attribute "%s" is required for "%s"'
                                 % (ma, resource))

        # get pilot and global sandbox
        resource_sandbox = self._session._get_resource_sandbox(pilot)
        session_sandbox  = self._session._get_session_sandbox (pilot)
        pilot_sandbox    = self._session._get_pilot_sandbox   (pilot)
        client_sandbox   = self._session._get_client_sandbox  ()

        pilot['resource_sandbox'] = str(resource_sandbox) % expand
        pilot['session_sandbox']  = str(session_sandbox)  % expand
        pilot['pilot_sandbox']    = str(pilot_sandbox)    % expand
        pilot['client_sandbox']   = str(client_sandbox)

        # from here on we need only paths
        resource_sandbox = resource_sandbox.path % expand
        session_sandbox  = session_sandbox .path % expand
        pilot_sandbox    = pilot_sandbox   .path % expand
      # client_sandbox   = client_sandbox  # not expanded

        if not job_name:
            job_name = pid

        try:
            if isinstance(agent_config, dict):
                agent_cfg = ru.Config(cfg=agent_config)

            elif isinstance(agent_config, str):
                agent_cfg = ru.Config('radical.pilot',
                                      category='agent',
                                      name=agent_config)
            else:
                # we can't handle this type
                raise TypeError('agent config must be string or dict')

        except Exception:
            self._log.exception('Error using agent config')
            raise


        # expand variables in virtenv string
        virtenv = virtenv % {'pilot_sandbox'   : pilot_sandbox,
                             'session_sandbox' : session_sandbox,
                             'resource_sandbox': resource_sandbox}

        # Check for deprecated global_virtenv
        if 'global_virtenv' in rcfg:
            raise RuntimeError("'global_virtenv' is deprecated (%s)" % resource)

        # Create a host:port string for use by the bootstrap_0.
        db_url = rs.Url(agent_dburl)
        if db_url.port:
            db_hostport = "%s:%d" % (db_url.host, db_url.port)
        else:
            db_hostport = "%s:%d" % (db_url.host, 27017)  # mongodb default

        # ----------------------------------------------------------------------
        # the version of the agent is derived from
        # rp_version, which has the following format
        # and interpretation:
        #
        # case rp_version:
        #   @<token>:
        #   @tag/@branch/@commit: # no sdist staging
        #       git clone $github_base radical.pilot.src
        #       (cd radical.pilot.src && git checkout token)
        #       pip install -t $VIRTENV/rp_install/ radical.pilot.src
        #       rm -rf radical.pilot.src
        #       export PYTHONPATH=$VIRTENV/rp_install:$PYTHONPATH
        #
        #   release: # no sdist staging
        #       pip install -t $VIRTENV/rp_install radical.pilot
        #       export PYTHONPATH=$VIRTENV/rp_install:$PYTHONPATH
        #
        #   local: # needs sdist staging
        #       tar zxf $sdist.tgz
        #       pip install -t $SANDBOX/rp_install $sdist/
        #       export PYTHONPATH=$SANDBOX/rp_install:$PYTHONPATH
        #
        #   installed: # no sdist staging
        #       true
        # esac
        #
        # virtenv_mode
        #   private : error  if ve exists, otherwise create, then use
        #   update  : update if ve exists, otherwise create, then use
        #   create  : use    if ve exists, otherwise create, then use
        #   use     : use    if ve exists, otherwise error,  then exit
        #   recreate: delete if ve exists, otherwise create, then use
        #   local   : use the client virtualenv (assumes same FS)
        #
        # examples   :
        #   virtenv@v0.20
        #   virtenv@devel
        #   virtenv@release
        #   virtenv@installed
        #   stage@local
        #   stage@/tmp/my_agent.py
        #
        # Note that some combinations may be invalid,
        # specifically in the context of virtenv_mode.  If, for
        # example, virtenv_mode is 'use', then the 'virtenv:tag'
        # will not make sense, as the virtenv is not updated.
        # In those cases, the virtenv_mode is honored, and
        # a warning is printed.
        #
        # Also, the 'stage' mode can only be combined with the
        # 'local' source, or with a path to the agent (relative
        # to root_dir, or absolute).
        #
        # A rp_version which does not adhere to the
        # above syntax is ignored, and the fallback stage@local
        # is used.

        if not rp_version:
            if virtenv_mode == 'local': rp_version = 'installed'
            else                      : rp_version = DEFAULT_RP_VERSION

        if not rp_version.startswith('@') and \
               rp_version not in ['installed', 'local', 'release']:
            raise ValueError("invalid rp_version '%s'" % rp_version)

        if rp_version.startswith('@'):
            rp_version  = rp_version[1:]  # strip '@'

        # use local VE ?
        if virtenv_mode == 'local':
            if os.environ.get('VIRTUAL_ENV'):
                python_dist = 'default'
                virtenv     = os.environ['VIRTUAL_ENV']
            elif os.environ.get('CONDA_PREFIX'):
                python_dist = 'anaconda'
                virtenv     = os.environ['CONDA_PREFIX']
            else:
                # we can't use local
                self._log.error('virtenv_mode is local, no local env found')
                raise ValueError('no local env found')

        # ----------------------------------------------------------------------
        # sanity checks
        if not python_dist     : raise RuntimeError("missing python distribution")
        if not virtenv_dist    : raise RuntimeError("missing virtualenv distribution")
        if not agent_spawner   : raise RuntimeError("missing agent spawner")
        if not agent_scheduler : raise RuntimeError("missing agent scheduler")
        if not resource_manager: raise RuntimeError("missing resource manager")

        # massage some values
        if not queue:
            queue = default_queue

        if  cleanup and isinstance(cleanup, bool):
            #  l : log files
            #  u : task work dirs
            #  v : virtualenv
            #  e : everything (== pilot sandbox)
            if shared_filesystem:
                cleanup = 'luve'
            else:
                # we cannot clean the sandbox from within the agent, as the hop
                # staging would then fail, and we'd get nothing back.
                # FIXME: cleanup needs to be done by the pmgr.launcher, or
                #        someone else, really, after fetching all logs and
                #        profiles.
                cleanup = 'luv'

            # we never cleanup virtenvs which are not private
            if virtenv_mode != 'private':
                cleanup = cleanup.replace('v', '')

        # if cores_per_node is set (!= None), then we need to
        # allocation full nodes, and thus round up
        if cores_per_node:
            cores_per_node = int(cores_per_node)
            number_cores   = int(cores_per_node *
                             math.ceil(float(number_cores) / cores_per_node))

        # if gpus_per_node is set (!= None), then we need to
        # allocation full nodes, and thus round up
        if gpus_per_node:
            gpus_per_node = int(gpus_per_node)
            number_gpus   = int(gpus_per_node *
                            math.ceil(float(number_gpus) / gpus_per_node))

        # set mandatory args
        bootstrap_args  = ""

        # add dists to staging files, if needed:
        # don't stage on `rp_version==installed` or `virtenv_mode==local`
        if rp_version   == 'installed' or \
           virtenv_mode == 'local'     :
            sdist_names = list()
            sdist_paths = list()
        else:
            sdist_names = [rg.sdist_name,
                           ru.sdist_name,
                           rs.sdist_name,
                           self._rp_sdist_name]
            sdist_paths = [rg.sdist_path,
                           ru.sdist_path,
                           rs.sdist_path,
                           self._rp_sdist_path]
            bootstrap_args += " -d '%s'" % (':'.join(sdist_names))

        bootstrap_args += " -p '%s'" % pid
        bootstrap_args += " -s '%s'" % sid
        bootstrap_args += " -m '%s'" % virtenv_mode
        bootstrap_args += " -r '%s'" % rp_version
        bootstrap_args += " -b '%s'" % python_dist
        bootstrap_args += " -g '%s'" % virtenv_dist
        bootstrap_args += " -v '%s'" % virtenv
        bootstrap_args += " -y '%d'" % runtime
        bootstrap_args += " -z '%s'" % tar_name

        # set optional args
        if resource_manager == "CCM": bootstrap_args += " -c"
        if forward_tunnel_endpoint:   bootstrap_args += " -f '%s'" % forward_tunnel_endpoint
        if forward_tunnel_endpoint:   bootstrap_args += " -h '%s'" % db_hostport
        if python_interpreter:        bootstrap_args += " -i '%s'" % python_interpreter
        if tunnel_bind_device:        bootstrap_args += " -t '%s'" % tunnel_bind_device
        if cleanup:                   bootstrap_args += " -x '%s'" % cleanup

        for arg in services:
            bootstrap_args += " -j '%s'" % arg
        for arg in pre_bootstrap_0:
            bootstrap_args += " -e '%s'" % arg
        for arg in pre_bootstrap_1:
            bootstrap_args += " -w '%s'" % arg

        agent_cfg['owner']               = 'agent.0'
        agent_cfg['resource']            = resource
        agent_cfg['cores']               = number_cores
        agent_cfg['gpus']                = number_gpus
        agent_cfg['spawner']             = agent_spawner
        agent_cfg['scheduler']           = agent_scheduler
        agent_cfg['runtime']             = runtime
        agent_cfg['app_comm']            = app_comm
        agent_cfg['dburl']               = str(database_url)
        agent_cfg['sid']                 = sid
        agent_cfg['pid']                 = pid
        agent_cfg['pmgr']                = self._pmgr
        agent_cfg['logdir']              = '.'
        agent_cfg['pilot_sandbox']       = pilot_sandbox
        agent_cfg['session_sandbox']     = session_sandbox
        agent_cfg['resource_sandbox']    = resource_sandbox
        agent_cfg['resource_manager']    = resource_manager
        agent_cfg['cores_per_node']      = cores_per_node
        agent_cfg['gpus_per_node']       = gpus_per_node
        agent_cfg['lfs_path_per_node']   = lfs_path_per_node
        agent_cfg['lfs_size_per_node']   = lfs_size_per_node
        agent_cfg['task_tmp']            = task_tmp
        agent_cfg['task_pre_launch']     = task_pre_launch
        agent_cfg['task_pre_exec']       = task_pre_exec
        agent_cfg['task_pre_rank']       = task_pre_rank
        agent_cfg['task_post_launch']    = task_post_launch
        agent_cfg['task_post_exec']      = task_post_exec
        agent_cfg['task_post_rank']      = task_post_rank
        agent_cfg['resource_cfg']        = copy.deepcopy(rcfg)
        agent_cfg['debug']               = self._log.getEffectiveLevel()

        # we'll also push the agent config into MongoDB
        pilot['cfg'] = agent_cfg

        # ----------------------------------------------------------------------
        # Write agent config dict to a json file in pilot sandbox.

        agent_cfg_name = 'agent.0.cfg'
        cfg_tmp_handle, cfg_tmp_file = tempfile.mkstemp(prefix='rp.agent_cfg.')
        os.close(cfg_tmp_handle)  # file exists now

        # Convert dict to json file
        self._log.debug("Write agent cfg to '%s'.", cfg_tmp_file)
        agent_cfg.write(cfg_tmp_file)

        # always stage agent cfg for each pilot, not in the tarball
        # FIXME: purge the tmp file after staging
        self._log.debug('cfg %s -> %s', agent_cfg['pid'], pilot_sandbox)
        ret['sds'].append({'source': cfg_tmp_file,
                           'target': '%s/%s' % (pilot['pilot_sandbox'], agent_cfg_name),
                           'action': rpc.TRANSFER})

        # always stage the bootstrapper for each pilot, not in the tarball
        # FIXME: this results in many staging ops for many pilots
        bootstrapper_path = os.path.abspath("%s/agent/bootstrap_0.sh"
                                           % self._root_dir)
        ret['sds'].append({'source': bootstrapper_path,
                           'target': '%s/bootstrap_0.sh' % pilot['pilot_sandbox'],
                           'action': rpc.TRANSFER})

        # always stage RU env helper
        env_helper = ru.which('radical-utils-env.sh')
        assert(env_helper)
        self._log.debug('env %s -> %s', env_helper, pilot_sandbox)
        ret['sds'].append({'source': env_helper,
                           'target': '%s/%s' % (pilot['pilot_sandbox'],
                                                os.path.basename(env_helper)),
                           'action': rpc.TRANSFER})

        # ----------------------------------------------------------------------
        # we also touch the log and profile tarballs in the target pilot sandbox
        ret['fts'].append({'src': '/dev/null',
                           'tgt': '%s/%s' % (pilot_sandbox, '%s.log.tgz' % pid),
                           'rem': False})  # don't remove /dev/null
        # only stage profiles if we profile
        if self._prof.enabled:
            ret['fts'].append({
                           'src': '/dev/null',
                           'tgt': '%s/%s' % (pilot_sandbox, '%s.prof.tgz' % pid),
                           'rem': False})  # don't remove /dev/null

        # check if we have a sandbox cached for that resource.  If so, we have
        # nothing to do.  Otherwise we create the sandbox and stage the RP
        # stack etc.
        #
        # NOTE: this will race when multiple pilot launcher instances are used!
        #
        if resource not in self._sandboxes:

            for sdist in sdist_paths:
                base = os.path.basename(sdist)
                ret['fts'].append({
                    'src': sdist,
                    'tgt': '%s/%s' % (session_sandbox, base),
                    'rem': False
                })

            # Some machines cannot run pip due to outdated CA certs.
            # For those, we also stage an updated certificate bundle
            # TODO: use booleans all the way?
            if stage_cacerts:

                certs = 'cacert.pem.gz'
                cpath = os.path.abspath("%s/agent/%s" % (self._root_dir, certs))
                self._log.debug("use CAs %s", cpath)

                ret['fts'].append({'src': cpath,
                                   'tgt': '%s/%s' % (session_sandbox, certs),
                                   'rem': False})

            self._sandboxes[resource] = True

        # ----------------------------------------------------------------------
        # Create SAGA Job description and submit the pilot job

        jd = rs.job.Description()

        jd.name                  = job_name
        jd.executable            = "/bin/bash"
        jd.arguments             = ['-l ./bootstrap_0.sh %s' % bootstrap_args]
        jd.working_directory     = pilot_sandbox
        jd.project               = project
        jd.output                = "bootstrap_0.out"
        jd.error                 = "bootstrap_0.err"
        jd.total_cpu_count       = number_cores
        jd.total_gpu_count       = number_gpus
        jd.total_physical_memory = required_memory
        jd.processes_per_host    = cores_per_node
        jd.spmd_variation        = spmd_variation
        jd.wall_time_limit       = runtime
        jd.queue                 = queue
        jd.candidate_hosts       = candidate_hosts
        jd.environment           = dict()
        jd.system_architecture   = dict(system_architecture)

        # register used resources in DB (enacted on next advance)
        pilot['resources'] = {'cpu': number_cores,
                              'gpu': number_gpus}
        pilot['$set']      = ['resources']


        # we set any saga_jd_supplement keys which are not already set above
        for key, val in saga_jd_supplement.items():
            if not jd[key]:
                self._log.debug('supplement %s: %s', key, val)
                jd[key] = val

        # set saga job description attribute based on env variable(s)
        if os.environ.get('RADICAL_SAGA_SMT'):
            try:
                jd.system_architecture['smt'] = \
                    int(os.environ['RADICAL_SAGA_SMT'])
            except Exception as e:
                self._log.debug('SAGA SMT not set: %s' % e)

        # job description environment variable(s) setup

        if self._prof.enabled:
            jd.environment['RADICAL_PROFILE'] = 'TRUE'

        jd.environment['RADICAL_BASE'] = resource_sandbox

        # for condor backends and the like which do not have shared FSs, we add
        # additional staging directives so that the backend system binds the
        # files from the session and pilot sandboxes to the pilot job.
        jd.file_transfer = list()
        if not shared_filesystem:

            jd.file_transfer.extend([
                'site:%s/%s > %s' % (pilot_sandbox,   agent_cfg_name, agent_cfg_name),
                'site:%s/%s.log.tgz > %s.log.tgz' % (pilot_sandbox, pid, pid),
                'site:%s/%s.log.tgz < %s.log.tgz' % (pilot_sandbox, pid, pid)
            ])

            if self._prof.enabled:
                jd.file_transfer.extend([
                    'site:%s/%s.prof.tgz > %s.prof.tgz' % (pilot_sandbox, pid, pid),
                    'site:%s/%s.prof.tgz < %s.prof.tgz' % (pilot_sandbox, pid, pid)
                ])

            for sdist in sdist_names:
                jd.file_transfer.extend([
                    'site:%s/%s > %s' % (session_sandbox, sdist, sdist)
                ])

            if stage_cacerts:
                jd.file_transfer.extend([
                    'site:%s/%s > %s' % (session_sandbox, certs, certs)
                ])

        self._log.debug("Bootstrap command line: %s %s", jd.executable, jd.arguments)

        ret['jd'] = jd
        return ret


    # --------------------------------------------------------------------------
    #
    def _stage_in(self, pilot, sds):
        '''
        Run some input staging directives.
        '''

        resource_sandbox = self._session._get_resource_sandbox(pilot)
      # session_sandbox  = self._session._get_session_sandbox (pilot)
        pilot_sandbox    = self._session._get_pilot_sandbox   (pilot)
        client_sandbox   = self._session._get_client_sandbox()

        # contexts for staging url expansion
        rem_ctx = {'pwd'     : pilot_sandbox,
                   'client'  : client_sandbox,
                   'pilot'   : pilot_sandbox,
                   'resource': resource_sandbox}

        loc_ctx = {'pwd'     : client_sandbox,
                   'client'  : client_sandbox,
                   'pilot'   : pilot_sandbox,
                   'resource': resource_sandbox}

        sds = ru.as_list(sds)

        for sd in sds:
            sd['prof_id'] = pilot['uid']
            sd['source'] = str(complete_url(sd['source'], loc_ctx, self._log))
            sd['target'] = str(complete_url(sd['target'], rem_ctx, self._log))

        self._stage(sds)


    # --------------------------------------------------------------------------
    #
    def _stage_out(self, pilot, sds):
        '''
        Run some output staging directives.
        '''

        resource_sandbox = self._session._get_resource_sandbox(pilot)
      # session_sandbox  = self._session._get_session_sandbox (pilot)
        pilot_sandbox    = self._session._get_pilot_sandbox   (pilot)
        client_sandbox   = self._session._get_client_sandbox()

        # contexts for staging url expansion
        loc_ctx = {'pwd'     : client_sandbox,
                   'client'  : client_sandbox,
                   'pilot'   : pilot_sandbox,
                   'resource': resource_sandbox}

        rem_ctx = {'pwd'     : pilot_sandbox,
                   'client'  : client_sandbox,
                   'pilot'   : pilot_sandbox,
                   'resource': resource_sandbox}

        sds = ru.as_list(sds)

        for sd in sds:
            sd['prof_id'] = pilot['uid']

        for sd in sds:
            sd['source'] = str(complete_url(sd['source'], rem_ctx, self._log))
            sd['target'] = str(complete_url(sd['target'], loc_ctx, self._log))

        self._stage(sds)


    # --------------------------------------------------------------------------
    #
    def _stage(self, sds):

        # add uid, ensure its a list, general cleanup
        sds  = expand_staging_directives(sds)
        uids = [sd['uid'] for sd in sds]

        # prepare to wait for completion
        with self._sds_lock:

            self._active_sds = dict()
            for sd in sds:
                sd['state'] = rps.NEW
                self._active_sds[sd['uid']] = sd

            sd_states = [sd['state'] for sd
                                     in  list(self._active_sds.values())
                                     if  sd['uid'] in uids]

        # push them out
        self._stager_queue.put(sds)

        while rps.NEW in sd_states:
            time.sleep(1.0)
            with self._sds_lock:
                sd_states = [sd['state'] for sd
                                         in  list(self._active_sds.values())
                                         if  sd['uid'] in uids]

        if rps.FAILED in sd_states:
            raise RuntimeError('pilot staging failed')


    # --------------------------------------------------------------------------
    #
    def _staging_ack_cb(self, topic, msg):
        '''
        update staging directive state information
        '''

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd == 'staging_result':

            with self._sds_lock:
                for sd in arg['sds']:
                    if sd['uid'] in self._active_sds:
                        self._active_sds[sd['uid']]['state'] = sd['state']

        return True


# ------------------------------------------------------------------------------
