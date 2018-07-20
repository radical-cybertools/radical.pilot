
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import math
import time
import pprint
import shutil
import tempfile
import threading

import subprocess           as sp

import saga                 as rs
import saga.filesystem      as rsfs
import radical.utils        as ru

from .... import pilot      as rp
from ...  import states     as rps
from ...  import constants  as rpc

from .base import PMGRLaunchingComponent

from ...staging_directives import complete_url
from ...staging_directives import TRANSFER, COPY, LINK, MOVE


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
BOOTSTRAPPER_0 = "bootstrap_0.sh"


# ==============================================================================
#
class Default(PMGRLaunchingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        PMGRLaunchingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        # we don't really have an output queue, as we pass control over the
        # pilot jobs to the resource management system (RM).

        self._pilots        = dict()             # dict for all known pilots
        self._pilots_lock   = threading.RLock()  # lock on maipulating the above
        self._checking      = list()             # pilots to check state on
        self._check_lock    = threading.RLock()  # lock on maipulating the above
        self._saga_fs_cache = dict()             # cache of saga directories
        self._saga_js_cache = dict()             # cache of saga job services
        self._sandboxes     = dict()             # cache of resource sandbox URLs
        self._cache_lock    = threading.RLock()  # lock for cache

        self._mod_dir       = os.path.dirname(os.path.abspath(__file__))
        self._root_dir      = "%s/../../"   % self._mod_dir  
        self._conf_dir      = "%s/configs/" % self._root_dir 

        self.register_input(rps.PMGR_LAUNCHING_PENDING, 
                            rpc.PMGR_LAUNCHING_QUEUE, self.work)

        # FIXME: make interval configurable
        self.register_timed_cb(self._pilot_watcher_cb, timer=10.0)

        # we listen for pilot cancel and input staging commands
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._pmgr_control_cb)

        self._log.info(ru.get_version([self._mod_dir, self._root_dir]))
        self._rp_version, _, _, _, self._rp_sdist_name, self._rp_sdist_path = \
                ru.get_version([self._mod_dir, self._root_dir])


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        # avoid shutdown races:

        self.unregister_timed_cb(self._pilot_watcher_cb)
        self.unregister_subscriber(rpc.CONTROL_PUBSUB, self._pmgr_control_cb)

        # FIXME: always kill all saga jobs for non-final pilots at termination,
        #        and set the pilot states to CANCELED.  This will confluct with
        #        disconnect/reconnect semantics.
        with self._pilots_lock:
            pids = self._pilots.keys()

        self._cancel_pilots(pids)
        self._kill_pilots(pids)

        with self._cache_lock:
            for url,js in self._saga_js_cache.iteritems():
                self._log.debug('close  js to %s', url)
                js.close()
                self._log.debug('closed js to %s', url)
            self._saga_js_cache.clear()
        self._log.debug('finalized child')


    # --------------------------------------------------------------------------
    #
    def _pmgr_control_cb(self, topic, msg):

        cmd = msg['cmd']
        arg = msg['arg']

        self._log.debug('launcher got %s', msg)

        if cmd == 'pilot_staging_input_request':

            self._handle_pilot_input_staging(arg['pilot'], arg['sds'])


        elif cmd == 'cancel_pilots':

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
    def _handle_pilot_input_staging(self, pilot, sds):

        pid = pilot['uid']

        # NOTE: no unit sandboxes defined!
        src_context = {'pwd'      : pilot['client_sandbox'],
                       'pilot'    : pilot['pilot_sandbox'],
                       'resource' : pilot['resource_sandbox']}
        tgt_context = {'pwd'      : pilot['pilot_sandbox'],
                       'pilot'    : pilot['pilot_sandbox'],
                       'resource' : pilot['resource_sandbox']}

        # Iterate over all directives
        for sd in sds:

            # TODO: respect flags in directive

            action = sd['action']
            flags  = sd['flags']
            did    = sd['uid']
            src    = sd['source']
            tgt    = sd['target']

            assert(action in [COPY, LINK, MOVE, TRANSFER])

            self._prof.prof('staging_in_start', uid=pid, msg=did)

            src = complete_url(src, src_context, self._log)
            tgt = complete_url(tgt, tgt_context, self._log)

            if action in [COPY, LINK, MOVE]:
                self._prof.prof('staging_in_fail', uid=pid, msg=did)
                raise ValueError("invalid action '%s' on pilot level" % action)

            self._log.info('transfer %s to %s', src, tgt)

            # FIXME: make sure that tgt URL points to the right resource
            # FIXME: honor sd flags if given (recursive...)
            flags = rsfs.CREATE_PARENTS

            # Define and open the staging directory for the pilot
            # We use the target dir construct here, so that we can create
            # the directory if it does not yet exist.

            # url used for cache (sandbox url w/o path)
            tmp      = rs.Url(pilot['pilot_sandbox'])
            tmp.path = '/'
            key = str(tmp)

            self._log.debug ("rs.file.Directory ('%s')", key)

            with self._cache_lock:
                if key in self._saga_fs_cache:
                    fs = self._saga_fs_cache[key]

                else:
                    fs = rsfs.Directory(key, session=self._session)
                    self._saga_fs_cache[key] = fs

            fs.copy(src, tgt, flags=flags)

            sd['pmgr_state'] = rps.DONE

            self._prof.prof('staging_in_stop', uid=pid, msg=did)

        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'pilot_staging_input_result', 
                                          'arg' : {'pilot' : pilot,
                                                   'sds'   : sds}})


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

        ru.raise_on('pilot_watcher_cb')

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
                self._log.debug('saga job state: %s %s', pid, state)

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

        # send the cancelation request to the pilots
        # FIXME: the cancellation request should not go directly to the DB, but
        #        through the DB abstraction layer...
        self._session._dbs.pilot_command('cancel_pilot', [], pids)
        self._log.debug('pilot(s).need(s) cancellation %s', pids)

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
                               for p in self._pilots.values()])
            last_cancel = max([p['pilot'].get('cancel_requested', 0) 
                               for p in self._pilots.values()])

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
                if pilot['state'] not in rp.FINAL:
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

                    if pilot['state'] in rp.FINAL:
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

                    self.advance(pilots, rps.PMGR_ACTIVE_PENDING, push=False, publish=True)

                except Exception:
                    self._log.exception('bulk launch failed')
                    self.advance(pilots, rps.FAILED, push=False, publish=True)


    # --------------------------------------------------------------------------
    #
    def _start_pilot_bulk(self, resource, schema, pilots):
        """
        For each pilot, we prepare by determining what files need to be staged,
        and what job description needs to be submitted.

        We expect `_prepare_pilot(resource, pilot)` to return a dict with:

            { 
              'js' : saga.job.Description,
              'ft' : [ 
                { 'src' : string  # absolute source file name
                  'tgt' : string  # relative target file name
                  'rem' : bool    # shall we remove src?
                }, 
                ... ]
            }

        When transfering data, we'll ensure that each src is only transferred
        once (in fact, we put all src files into a tarball and unpack that on
        the target side).

        The returned dicts are expected to only contain files which actually
        need staging, ie. which have not been staged during a previous pilot
        submission.  That implies one of two things: either this component is
        stateful, and remembers what has been staged -- which makes it difficult
        to use multiple component instances; or the component inspects the
        target resource for existing files -- which involves additional
        expensive remote hops.
        FIXME: since neither is implemented at this point we won't discuss the
               tradeoffs further -- right now files are unique per pilot bulk.

        Once all dicts are collected, we create one additional file which
        contains the staging information, and then pack all src files into
        a tarball for staging.  We transfer the tarball, and *immediately*
        trigger the untaring on the target resource, which is thus *not* part of
        the bootstrapping process.
        NOTE: this is to avoid untaring race conditions for multiple pilots, and
              also to simplify bootstrapping dependencies -- the bootstrappers
              are likely within the tarball after all...
        """

        rcfg = self._session.get_resource_config(resource, schema)
        sid  = self._session.uid

        # we create a fake session_sandbox with all pilot_sandboxes in /tmp, and
        # then tar it up.  Once we untar that tarball on the target machine, we
        # should have all sandboxes and all files required to bootstrap the
        # pilots
        # FIXME: on untar, there is a race between multiple launcher components
        #        within the same session toward the same target resource.
        tmp_dir  = os.path.abspath(tempfile.mkdtemp(prefix='rp_agent_tar_dir'))
        tar_name = '%s.%s.tgz' % (sid, self.uid)
        tar_tgt  = '%s/%s'     % (tmp_dir, tar_name)
        tar_url  = rs.Url('file://localhost/%s' % tar_tgt)

        # we need the session sandbox url, but that is (at least in principle)
        # dependent on the schema to use for pilot startup.  So we confirm here
        # that the bulk is consistent wrt. to the schema.
        # FIXME: if it is not, it needs to be splitted into schema-specific
        # sub-bulks
        schema = pilots[0]['description'].get('access_schema')
        for pilot in pilots[1:]:
            assert(schema == pilot['description'].get('access_schema')), \
                    'inconsistent scheme on launch / staging'

        session_sandbox = self._session._get_session_sandbox(pilots[0]).path


        # we will create the session sandbox before we untar, so we can use that
        # as workdir, and pack all paths relative to that session sandbox.  That
        # implies that we have to recheck that all URLs in fact do point into
        # the session sandbox.

        ft_list = list()  # files to stage
        jd_list = list()  # jobs  to submit
        for pilot in pilots:
            info = self._prepare_pilot(resource, rcfg, pilot)
            ft_list += info['ft']
            jd_list.append(info['jd'])
            self._prof.prof('staging_in_start', uid=pilot['uid'])

        for ft in ft_list:
            src     = os.path.abspath(ft['src'])
            tgt     = os.path.relpath(os.path.normpath(ft['tgt']), session_sandbox)
          # src_dir = os.path.dirname(src)
            tgt_dir = os.path.dirname(tgt)

            if tgt_dir.startswith('..'):
                raise ValueError('staging target %s outside of pilot sandbox' % ft['tgt'])

            if not os.path.isdir('%s/%s' % (tmp_dir, tgt_dir)):
                os.makedirs('%s/%s' % (tmp_dir, tgt_dir))

            if src == '/dev/null' :
                # we want an empty file -- touch it (tar will refuse to 
                # handle a symlink to /dev/null)
                open('%s/%s' % (tmp_dir, tgt), 'a').close()
            else:
                os.symlink(src, '%s/%s' % (tmp_dir, tgt))

        # tar.  If any command fails, this will raise.
        cmd = "cd %s && tar zchf %s *" % (tmp_dir, tar_tgt)
        self._log.debug('cmd: %s', cmd)
        try:
            out = sp.check_output(["/bin/sh", "-c", cmd], stderr=sp.STDOUT)
        except Exception:
            self._log.exception('callout failed: %s', out)
            raise
        else:
            self._log.debug('out: %s', out)

        # remove all files marked for removal-after-pack
        for ft in ft_list:
            if ft['rem']:
                os.unlink(ft['src'])

        fs_endpoint = rcfg['filesystem_endpoint']
        fs_url      = rs.Url(fs_endpoint)

        self._log.debug ("rs.file.Directory ('%s')", fs_url)

        with self._cache_lock:
            if fs_url in self._saga_fs_cache:
                fs = self._saga_fs_cache[fs_url]
            else:
                fs = rsfs.Directory(fs_url, session=self._session)
                self._saga_fs_cache[fs_url] = fs

        tar_rem      = rs.Url(fs_url)
        tar_rem.path = "%s/%s" % (session_sandbox, tar_name)

        fs.copy(tar_url, tar_rem, flags=rsfs.CREATE_PARENTS)

        shutil.rmtree(tmp_dir)

        # we now need to untar on the target machine.
        js_url = ru.Url(pilots[0]['js_url'])

        # well, we actually don't need to talk to the lrms, but only need
        # a shell on the headnode.  That seems true for all LRMSs we use right
        # now.  So, lets convert the URL:
        if '+' in js_url.scheme:
            parts = js_url.scheme.split('+')
            if 'gsissh' in parts: js_url.scheme = 'gsissh'
            elif  'ssh' in parts: js_url.scheme = 'ssh'
        else:
            # In the non-combined '+' case we need to distinguish between
            # a url that was the result of a hop or a local lrms.
            if js_url.scheme not in ['ssh', 'gsissh']:
                js_url.scheme = 'fork'

        with self._cache_lock:
            if  js_url in self._saga_js_cache:
                js_tmp  = self._saga_js_cache[js_url]
            else:
                js_tmp  = rs.job.Service(js_url, session=self._session)
                self._saga_js_cache[js_url] = js_tmp

     ## cmd = "tar zmxvf %s/%s -C / ; rm -f %s" % \
        cmd = "tar zmxvf %s/%s -C %s" % \
                (session_sandbox, tar_name, session_sandbox)
        j = js_tmp.run_job(cmd)
        j.wait()

        self._log.debug('tar cmd : %s', cmd)
        self._log.debug('tar done: %s, %s, %s', j.state, j.stdout, j.stderr)

        for pilot in pilots:
            self._prof.prof('staging_in_stop', uid=pilot['uid'])
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

        for j in jc.get_tasks():

            # do a quick error check
            if j.state == rs.FAILED:
                self._log.error('%s: %s : %s : %s', j.id, j.state, j.stderr, j.stdout)
                raise RuntimeError ("SAGA Job state is FAILED.")

            if not j.name:
                raise RuntimeError('cannot get job name for %s' % j.id)

            pilot = None
            for p in pilots:
                if p['uid'] == j.name:
                    pilot = p
                    break

            if not pilot:
                raise RuntimeError('job does not match any pilot: %s : %s'
                                  % (j.name, j.id))

            pid = pilot['uid']
            self._log.debug('pilot job: %s : %s : %s : %s', 
                            pid, j.id, j.name, j.state)

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
    def _prepare_pilot(self, resource, rcfg, pilot):

        pid = pilot["uid"]
        ret = {'ft' : list(),
               'jd' : None  }

      # # ----------------------------------------------------------------------
      # # the rcfg can contain keys with string expansion placeholders where
      # # values from the pilot description need filling in.  A prominent
      # # example is `%(pd.project)s`, where the pilot description's `PROJECT`
      # # value needs to be filled in (here in lowercase).
      # expand = dict()
      # for k,v in pilot['description'].iteritems():
      #     if v is None:
      #         v = ''
      #     expand['pd.%s' % k] = v
      #     if isinstance(v, basestring):
      #         expand['pd.%s' % k.upper()] = v.upper()
      #         expand['pd.%s' % k.lower()] = v.lower()
      #     else:
      #         expand['pd.%s' % k.upper()] = v
      #         expand['pd.%s' % k.lower()] = v
      #
      # for k in rcfg:
      #     if isinstance(rcfg[k], basestring):
      #         orig     = rcfg[k]
      #         rcfg[k]  = rcfg[k] % expand
      #         expanded = rcfg[k]
      #         if orig != expanded:
      #             self._log.debug('RCFG:\n%s\n%s', orig, expanded)

        # ----------------------------------------------------------------------
        # Database connection parameters
        sid           = self._session.uid
        database_url  = self._session.dburl

        # some default values are determined at runtime
        default_virtenv = '%%(resource_sandbox)s/ve.%s.%s' % \
                          (resource, self._rp_version)

        # ----------------------------------------------------------------------
        # pilot description and resource configuration
        number_cores    = pilot['description']['cores']
        number_gpus     = pilot['description']['gpus']
        runtime         = pilot['description']['runtime']
        queue           = pilot['description']['queue']
        project         = pilot['description']['project']
        cleanup         = pilot['description']['cleanup']
        memory          = pilot['description']['memory']
        candidate_hosts = pilot['description']['candidate_hosts']

        # ----------------------------------------------------------------------
        # get parameters from resource cfg, set defaults where needed
        agent_launch_method     = rcfg.get('agent_launch_method')
        agent_dburl             = rcfg.get('agent_mongodb_endpoint', database_url)
        agent_spawner           = rcfg.get('agent_spawner',       DEFAULT_AGENT_SPAWNER)
        rc_agent_config         = rcfg.get('agent_config',        DEFAULT_AGENT_CONFIG)
        agent_scheduler         = rcfg.get('agent_scheduler')
        tunnel_bind_device      = rcfg.get('tunnel_bind_device')
        default_queue           = rcfg.get('default_queue')
        forward_tunnel_endpoint = rcfg.get('forward_tunnel_endpoint')
        lrms                    = rcfg.get('lrms')
        mpi_launch_method       = rcfg.get('mpi_launch_method', '')
        pre_bootstrap_0         = rcfg.get('pre_bootstrap_0', [])
        pre_bootstrap_1         = rcfg.get('pre_bootstrap_1', [])
        python_interpreter      = rcfg.get('python_interpreter')
        task_launch_method      = rcfg.get('task_launch_method')
        rp_version              = rcfg.get('rp_version',          DEFAULT_RP_VERSION)
        virtenv_mode            = rcfg.get('virtenv_mode',        DEFAULT_VIRTENV_MODE)
        virtenv                 = rcfg.get('virtenv',             default_virtenv)
        cores_per_node          = rcfg.get('cores_per_node', 0)
        gpus_per_node           = rcfg.get('gpus_per_node',  0)
        lfs_path_per_node       = rcfg.get('lfs_path_per_node', None)
        lfs_size_per_node       = rcfg.get('lfs_size_per_node',  0)
        python_dist             = rcfg.get('python_dist')
        virtenv_dist            = rcfg.get('virtenv_dist',        DEFAULT_VIRTENV_DIST)
        cu_tmp                  = rcfg.get('cu_tmp')
        spmd_variation          = rcfg.get('spmd_variation')
        shared_filesystem       = rcfg.get('shared_filesystem', True)
        stage_cacerts           = rcfg.get('stage_cacerts', False)
        cu_pre_exec             = rcfg.get('cu_pre_exec')
        cu_post_exec            = rcfg.get('cu_post_exec')
        export_to_cu            = rcfg.get('export_to_cu')
        mandatory_args          = rcfg.get('mandatory_args', [])
        saga_jd_supplement      = rcfg.get('saga_jd_supplement', {})

        import pprint
        self._log.debug(cores_per_node)
        self._log.debug(pprint.pformat(rcfg))

        # make sure that mandatory args are known
        for ma in mandatory_args:
            if pilot['description'].get(ma) is None:
                raise  ValueError('attribute "%s" is required for "%s"'
                                 % (ma, resource))

        # get pilot and global sandbox
        resource_sandbox = self._session._get_resource_sandbox (pilot).path
        session_sandbox  = self._session._get_session_sandbox(pilot).path
        pilot_sandbox    = self._session._get_pilot_sandbox  (pilot).path

        pilot['resource_sandbox'] = str(self._session._get_resource_sandbox(pilot))
        pilot['pilot_sandbox']    = str(self._session._get_pilot_sandbox(pilot))
        pilot['client_sandbox']   = str(self._session._get_client_sandbox())

        # Agent configuration that is not part of the public API.
        # The agent config can either be a config dict, or
        # a string pointing to a configuration name.  If neither
        # is given, check if 'RADICAL_PILOT_AGENT_CONFIG' is
        # set.  The last fallback is 'agent_default'
        agent_config = pilot['description'].get('_config')
        if not agent_config:
            agent_config = os.environ.get('RADICAL_PILOT_AGENT_CONFIG')
        if not agent_config:
            agent_config = rc_agent_config

        if isinstance(agent_config, dict):

            # use dict as is
            agent_cfg = agent_config

        elif isinstance(agent_config, basestring):
            try:
                # interpret as a config name
                agent_cfg_file = os.path.join(self._conf_dir, "agent_%s.json" % agent_config)

                self._log.info("Read agent config file: %s",  agent_cfg_file)
                agent_cfg = ru.read_json(agent_cfg_file)

                # allow for user level overload
                user_cfg_file = '%s/.radical/pilot/config/%s' \
                              % (os.environ['HOME'], os.path.basename(agent_cfg_file))

                if os.path.exists(user_cfg_file):
                    self._log.info("merging user config: %s" % user_cfg_file)
                    user_cfg = ru.read_json(user_cfg_file)
                    ru.dict_merge (agent_cfg, user_cfg, policy='overwrite')

            except Exception as e:
                self._log.exception("Error reading agent config file: %s" % e)
                raise

        else:
            # we can't handle this type
            raise TypeError('agent config must be string (config name) or dict')

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
        #       pip install -t $VIRTENV/rp_install $sdist/
        #       export PYTHONPATH=$VIRTENV/rp_install:$PYTHONPATH
        #
        #   debug: # needs sdist staging
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

        if  not rp_version.startswith('@') and \
            not rp_version in ['installed', 'local', 'debug', 'release']:
            raise ValueError("invalid rp_version '%s'" % rp_version)

        if rp_version.startswith('@'):
            rp_version  = rp_version[1:]  # strip '@'


        # ----------------------------------------------------------------------
        # sanity checks
        if not python_dist        : raise RuntimeError("missing python distribution")
        if not virtenv_dist       : raise RuntimeError("missing virtualenv distribution")
        if not agent_spawner      : raise RuntimeError("missing agent spawner")
        if not agent_scheduler    : raise RuntimeError("missing agent scheduler")
        if not lrms               : raise RuntimeError("missing LRMS")
        if not agent_launch_method: raise RuntimeError("missing agentlaunch method")
        if not task_launch_method : raise RuntimeError("missing task launch method")

        # massage some values
        if not queue :
            queue = default_queue

        if  cleanup and isinstance (cleanup, bool) :
            #  l : log files
            #  u : unit work dirs
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
            if virtenv_mode is not 'private' :
                cleanup = cleanup.replace ('v', '')

        # add dists to staging files, if needed
        if rp_version in ['local', 'debug']:
            sdist_names = [ru.sdist_name, rs.sdist_name, self._rp_sdist_name]
            sdist_paths = [ru.sdist_path, rs.sdist_path, self._rp_sdist_path]
        else:
            sdist_names = list()
            sdist_paths = list()

        # if cores_per_node is set (!= None), then we need to
        # allocation full nodes, and thus round up
        if cores_per_node:
            cores_per_node = int(cores_per_node)
            number_cores   = int(cores_per_node
                           * math.ceil(float(number_cores) / cores_per_node))

        # if gpus_per_node is set (!= None), then we need to
        # allocation full nodes, and thus round up
        if gpus_per_node:
            gpus_per_node = int(gpus_per_node)
            number_gpus   = int(gpus_per_node
                           * math.ceil(float(number_gpus) / gpus_per_node))

        # set mandatory args
        bootstrap_args  = ""
        bootstrap_args += " -d '%s'" % ':'.join(sdist_names)
        bootstrap_args += " -p '%s'" % pid
        bootstrap_args += " -s '%s'" % sid
        bootstrap_args += " -m '%s'" % virtenv_mode
        bootstrap_args += " -r '%s'" % rp_version
        bootstrap_args += " -b '%s'" % python_dist
        bootstrap_args += " -g '%s'" % virtenv_dist
        bootstrap_args += " -v '%s'" % virtenv
        bootstrap_args += " -y '%d'" % runtime

        # set optional args
        if lrms == "CCM":           bootstrap_args += " -c"
        if forward_tunnel_endpoint: bootstrap_args += " -f '%s'" % forward_tunnel_endpoint
        if forward_tunnel_endpoint: bootstrap_args += " -h '%s'" % db_hostport
        if python_interpreter:      bootstrap_args += " -i '%s'" % python_interpreter
        if tunnel_bind_device:      bootstrap_args += " -t '%s'" % tunnel_bind_device
        if cleanup:                 bootstrap_args += " -x '%s'" % cleanup

        for arg in pre_bootstrap_0:
            bootstrap_args += " -e '%s'" % arg
        for arg in pre_bootstrap_1:
            bootstrap_args += " -w '%s'" % arg

        agent_cfg['owner']              = 'agent_0'
        agent_cfg['cores']              = number_cores
        agent_cfg['gpus']               = number_gpus
        agent_cfg['lrms']               = lrms
        agent_cfg['spawner']            = agent_spawner
        agent_cfg['scheduler']          = agent_scheduler
        agent_cfg['runtime']            = runtime
        agent_cfg['dburl']              = str(database_url)
        agent_cfg['session_id']         = sid
        agent_cfg['pilot_id']           = pid
        agent_cfg['logdir']             = '.'
        agent_cfg['pilot_sandbox']      = pilot_sandbox
        agent_cfg['session_sandbox']    = session_sandbox
        agent_cfg['resource_sandbox']   = resource_sandbox
        agent_cfg['agent_launch_method']= agent_launch_method
        agent_cfg['task_launch_method'] = task_launch_method
        agent_cfg['mpi_launch_method']  = mpi_launch_method
        agent_cfg['cores_per_node']     = cores_per_node
        agent_cfg['gpus_per_node']      = gpus_per_node
        agent_cfg['lfs_path_per_node']  = lfs_path_per_node
        agent_cfg['lfs_size_per_node']  = lfs_size_per_node
        agent_cfg['cu_tmp']             = cu_tmp
        agent_cfg['export_to_cu']       = export_to_cu
        agent_cfg['cu_pre_exec']        = cu_pre_exec
        agent_cfg['cu_post_exec']       = cu_post_exec
        agent_cfg['resource_cfg']       = copy.deepcopy(rcfg)
        agent_cfg['debug']              = self._log.getEffectiveLevel()

        # we'll also push the agent config into MongoDB
        pilot['cfg'] = agent_cfg

        # ----------------------------------------------------------------------
        # Write agent config dict to a json file in pilot sandbox.

        agent_cfg_name = 'agent_0.cfg'
        cfg_tmp_handle, cfg_tmp_file = tempfile.mkstemp(prefix='rp.agent_cfg.')
        os.close(cfg_tmp_handle)  # file exists now

        # Convert dict to json file
        self._log.debug("Write agent cfg to '%s'.", cfg_tmp_file)
        self._log.debug(pprint.pformat(agent_cfg))
        ru.write_json(agent_cfg, cfg_tmp_file)

        ret['ft'].append({'src' : cfg_tmp_file, 
                          'tgt' : '%s/%s' % (pilot_sandbox, agent_cfg_name),
                          'rem' : True})  # purge the tmp file after packing

        # ----------------------------------------------------------------------
        # we also touch the log and profile tarballs in the target pilot sandbox
        ret['ft'].append({'src' : '/dev/null',
                          'tgt' : '%s/%s' % (pilot_sandbox, '%s.log.tgz' % pid),
                          'rem' : False})  # don't remove /dev/null
        # only stage profiles if we profile
        if self._prof.enabled:
            ret['ft'].append({
                          'src' : '/dev/null',
                          'tgt' : '%s/%s' % (pilot_sandbox, '%s.prof.tgz' % pid),
                          'rem' : False})  # don't remove /dev/null

        # check if we have a sandbox cached for that resource.  If so, we have
        # nothing to do.  Otherwise we create the sandbox and stage the RP
        # stack etc.
        # NOTE: this will race when multiple pilot launcher instances are used!
        with self._cache_lock:

            if resource not in self._sandboxes:

                for sdist in sdist_paths:
                    base = os.path.basename(sdist)
                    ret['ft'].append({'src' : sdist, 
                                      'tgt' : '%s/%s' % (session_sandbox, base),
                                      'rem' : False})

                # Copy the bootstrap shell script.
                bootstrapper_path = os.path.abspath("%s/agent/%s"
                                  % (self._root_dir, BOOTSTRAPPER_0))
                self._log.debug("use bootstrapper %s", bootstrapper_path)

                ret['ft'].append({'src' : bootstrapper_path, 
                                  'tgt' : '%s/%s' % (session_sandbox, BOOTSTRAPPER_0),
                                  'rem' : False})

                # Some machines cannot run pip due to outdated CA certs.
                # For those, we also stage an updated certificate bundle
                # TODO: use booleans all the way?
                if stage_cacerts:

                    cc_name = 'cacert.pem.gz'
                    cc_path = os.path.abspath("%s/agent/%s" % (self._root_dir, cc_name))
                    self._log.debug("use CAs %s", cc_path)

                    ret['ft'].append({'src' : cc_path, 
                                      'tgt' : '%s/%s' % (session_sandbox, cc_name),
                                      'rem' : False})

                self._sandboxes[resource] = True


        # ----------------------------------------------------------------------
        # Create SAGA Job description and submit the pilot job

        jd = rs.job.Description()

        if shared_filesystem:
            bootstrap_tgt = '%s/%s' % (session_sandbox, BOOTSTRAPPER_0)
        else:
            bootstrap_tgt = '%s/%s' % ('.', BOOTSTRAPPER_0)

        jd.name                  = pid
        jd.executable            = "/bin/bash"
        jd.arguments             = ['-l %s' % bootstrap_tgt, bootstrap_args]
        jd.working_directory     = pilot_sandbox
        jd.project               = project
        jd.output                = "bootstrap_0.out"
        jd.error                 = "bootstrap_0.err"
        jd.total_cpu_count       = number_cores
        jd.total_gpu_count       = number_gpus
        jd.processes_per_host    = cores_per_node
        jd.spmd_variation        = spmd_variation
        jd.wall_time_limit       = runtime
        jd.total_physical_memory = memory
        jd.queue                 = queue
        jd.candidate_hosts       = candidate_hosts
        jd.environment           = dict()

        # we set any saga_jd_supplement keys which are not already set above
        for key, val in saga_jd_supplement.iteritems():
            if not jd[key]:
                self._log.debug('supplement %s: %s', key, val)
                jd[key] = val

        if 'RADICAL_PILOT_PROFILE' in os.environ :
            jd.environment['RADICAL_PILOT_PROFILE'] = 'TRUE'

        # for condor backends and the like which do not have shared FSs, we add
        # additional staging directives so that the backend system binds the
        # files from the session and pilot sandboxes to the pilot job.
        jd.file_transfer = list()
        if not shared_filesystem:

            jd.file_transfer.extend([
                'site:%s/%s > %s' % (session_sandbox, BOOTSTRAPPER_0, BOOTSTRAPPER_0),
                'site:%s/%s > %s' % (pilot_sandbox,   agent_cfg_name, agent_cfg_name),
                'site:%s/%s.log.tgz > %s.log.tgz' % (pilot_sandbox, pid, pid),
                'site:%s/%s.log.tgz < %s.log.tgz' % (pilot_sandbox, pid, pid)
            ])

            if 'RADICAL_PILOT_PROFILE' in os.environ:
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
                    'site:%s/%s > %s' % (session_sandbox, cc_name, cc_name)
                ])

        self._log.debug("Bootstrap command line: %s %s", jd.executable, jd.arguments)

        ret['jd'] = jd
        return ret


# ------------------------------------------------------------------------------

