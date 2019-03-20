
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import math
import time
import pprint
import shutil
import tempfile
import threading as mt

import subprocess           as sp

import radical.saga         as rs
import radical.utils        as ru

from .... import pilot      as rp
from ...  import states     as rps
from ...  import constants  as rpc

from .base import PMGRLaunchingComponent

from ...staging_directives import complete_url

rsfs = rs.filesystem


# TODO: perform all file staging as RS job input/output staging directivees.
#       If needed, implement tar and cache support in RS.  Just don't specify
#       same file staging op twice in same session.

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

LOCAL_SCHEME = 'file'
BOOTSTRAP_0  = "bootstrap_0.sh"


# ------------------------------------------------------------------------------
#
class Single(PMGRLaunchingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        PMGRLaunchingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # we don't really have an output queue, as we pass control over the
        # pilot jobs to the resource management system (RM).

        self._pilots        = dict()      # dict for all known pilots
        self._pilots_lock   = mt.RLock()  # lock on maipulating the above
        self._checking      = list()      # pilots to check state on
        self._check_lock    = mt.RLock()  # lock on maipulating the above
        self._saga_fs_cache = dict()      # cache of saga directories
        self._saga_js_cache = dict()      # cache of saga job services
        self._sbox          = dict()      # cache of resource sandbox URLs
        self._cache_lock    = mt.RLock()  # lock for cache

        self._mod_dir       = os.path.dirname(os.path.abspath(__file__))
        self._root_dir      = "%s/../../"   % self._mod_dir  
        self._conf_dir      = "%s/configs/" % self._root_dir 

        self._pmgr          = self._cfg['owner']

        self.register_input(rps.PMGR_LAUNCHING_PENDING, 
                            rpc.PMGR_LAUNCHING_QUEUE, self.work)

        # FIXME: make interval configurable
        self._pilot_watcher_period = 3.0
        self.register_timed_cb(self._pilot_watcher_cb, 
                               timer=self._pilot_watcher_period)

        # we listen for pilot cancel and input staging commands
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._pmgr_control_cb)

        self._log.info(ru.get_version([self._mod_dir, self._root_dir]))
        self._rp_version, _, _, _, self._rp_sdist_name, self._rp_sdist_path = \
                ru.get_version([self._mod_dir, self._root_dir])


    # --------------------------------------------------------------------------
    #
    def client_notify(self, msg, cb_data):

        self._log.info('got notification: %s from %s', msg, cb_data['pid'])

        self.publish(rpc.STATE_PUBSUB, msg)


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # avoid shutdown races:

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

            assert(action in [rpc.COPY, rpc.LINK, rpc.MOVE, rpc.TRANSFER])

            self._prof.prof('staging_in_start', uid=pid, msg=did)

            src = complete_url(src, src_context, self._log)
            tgt = complete_url(tgt, tgt_context, self._log)

            if action in [rpc.COPY, rpc.LINK, rpc.MOVE]:
                self._prof.prof('staging_in_fail', uid=pid, msg=did)
                raise ValueError("invalid action '%s' on pilot level" % action)

            self._log.info('transfer %s to %s', src, tgt)

            # FIXME: make sure that tgt URL points to the right resource
            # FIXME: honor sd flags if given (recursive...)
            flags = rsfs.CREATE_PARENTS

            if os.path.isdir(src.path):
                flags |= rsfs.RECURSIVE

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

        # TODO: use SAGA job state notifications
        # TODO: use pilot heartbeats

        # send heartbeats to all pilots
        # FIXME: this needs to go to *all* pilots
        self.publish(rpc.AGENT_PUBSUB, {'cmd': 'heartbeat',
                                        'arg': {'uid' : self._uid,
                                                'time': time.time()}})

        # check heartbeats from pilots
        # FIXME: this goes into the pilot's agent pubsub subscriber
        # pilot.heartbeat.beat()

        # FIXME: check bridge health
        #        bridges should havea  configured timeout after which they die
        #        if no traffic has been seen from either end.  This should be
        #        twice the heartbeat timeout or something.

        return True


        final_pilots = list()
        live_pilots  = list()

        with self._pilots_lock, self._check_lock:

            for pid in self._checking:

                state = self._pilots[pid]['job'].state
                pilot = self._pilots[pid]['pilot']
                self._log.debug('saga job state: %s %s', pid, state)

                if state not in [rs.job.DONE, rs.job.FAILED, rs.job.CANCELED]:
                    live_pilots.append(pilot)
                else:
                    if state == rs.job.DONE    : pilot['state'] = rps.DONE
                    if state == rs.job.FAILED  : pilot['state'] = rps.FAILED
                    if state == rs.job.CANCELED: pilot['state'] = rps.CANCELED
                    final_pilots.append(pilot)

        if final_pilots:
            for pilot in final_pilots:
                with self._check_lock:
                    # stop monitoring this pilot
                    self._checking.remove(pilot['uid'])

                self._log.debug('final %s [%s]', pilot['uid'], pilot['state'])

            self.advance(final_pilots, push=False, publish=True)


        if live_pilots:

            # send a heartbeat
            pids = [pilot['uid'] for pilot in live_pilots]
            arg  = {'period' : self._pilot_watcher_period, 
                    'tstamp' : time.time()}

            self._log.debug('live pilots: %s', pids)

            for pid in pids:

                # FIXME: use control pubsub?  But then updater needs to listen
                #        on that...
                self.publish(rpc.STATE_PUBSUB, {'cmd' :  'cmd', 
                                                'arg' : {'type': 'pilot', 
                                                         'uid' :  pid, 
                                                         'cmd' : 'heartbeat', 
                                                         'arg' :  arg}})


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

        # send the cancelation request to the pilots (via the update worker)
        with self._pilots_lock:

            for pid in pids:

                if pid not in self._pilots:
                    self._log.error('unknown: %s', pid)
                    raise ValueError('unknown pilot %s' % pid)

                if self._pilots[pid]['pilot'].get('cancel_requested'):
                    self._log.debug('pilot %s was canceled already', pid)
                    continue

                # TODO: send CONTROL (not STATE) message
                self._log.info('cancel pilot %s', pid)
                self.publish(rpc.STATE_PUBSUB, {'cmd':  'cmd', 
                                                'arg': {'type': 'pilot', 
                                                        'uid' :  pid,
                                                        'cmd' : 'cancel_pilot', 
                                                        'arg' :  None}})
                self._pilots[pid]['pilot']['cancel_requested'] = True


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

        if not pids:
            pids = [p.uid for p in self._pilots]

        # we don't want the watcher checking for these pilot anymore
        with self._check_lock:
            for pid in pids:
                if pid in self._checking:
                    self._checking.remove(pid)

        alive_pids = list()
        to_advance = list()
        with self._pilots_lock:

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


                self._log.debug('killing pilot: %s', pid)

                to_advance.append(self._pilots[pid]['pilot'])

                self._pilots[pid]['job'].cancel()
                self._pilots[pid]['job'].wait()

            # set canceled state
            self.advance(to_advance, state=rps.CANCELED,
                         push=False, publish=True)

        return True


    # --------------------------------------------------------------------------
    #
    def work(self, pilots):

        self.advance(pilots, rps.PMGR_LAUNCHING, publish=True, push=False)

        for pilot in pilots:

            self._log.info("Launching pilot %s", pilot['uid'])

            rcfg = self._session.get_resource_config(pilot['description'])
            acfg = self._session.get_agent_config   (pilot['description'], rcfg)
            pid  = pilot['uid']

            jdp, jdb = self._prepare_pilot(rcfg, acfg, pilot)
            self._prof.prof('staging_in_start', uid=pid)

            self._start_bridges(pilot, rcfg, jdb)
            self._start_pilot  (pilot, rcfg, jdp)

        self.advance(pilots, rps.PMGR_ACTIVE_PENDING, push=False, publish=True)


    # --------------------------------------------------------------------------
    #
    def _start_bridges(self, pilot, rcfg, jdb):
        '''
        start the bridges defined in the bridge job descriptions
        '''

        pid = pilot['uid']
        self._prof.prof('bridge_submission_start', uid=pid)

        # look up or create JS for actual pilot submission.  This might result
        # in the same js url as above, or not.
        js_ep  = rcfg['access']['bridge_ep']
        with self._cache_lock:
            if js_ep in self._saga_js_cache:
                js = self._saga_js_cache[js_ep]
            else:
                js = rs.job.Service(js_ep, session=self._session)
                self._saga_js_cache[js_ep] = js

        self._log.debug('jdp: %s', pprint.pformat(jdb.as_dict()))
        job = js.create_job(jdb)
        job.run()

        # check for submission errors (startup e rrors won't be caught)
        if job.state == rs.FAILED:
            self._log.error('%s: %s : %s : %s', 
                            job.id, job.state, job.stderr, job.stdout)
            raise RuntimeError ("SAGA Job state is FAILED. (%s)" % jdb.name)

        # connect to the pilot's notification channel to obtain state
        # notifications, which are then forwarded to the local state pubsub.
        self.register_subscriber(rpc.AGENT_PUBSUB, cb=self._agent_pubsub,
                                                   cb_data={'pid': pid})

        # FIXME: do we need to keep bridge jobs around?
        self._prof.prof('bridge_submission_stop', uid=pid)



    # --------------------------------------------------------------------------
    #
    def _start_pilot(self, pilot, rcfg, jdp):
        '''
        For the given pilot, submit the job description.
        '''

        pid = pilot['uid']

        self._prof.prof('submission_start', uid=pid)

        # look up or create JS for actual pilot submission.  This might result
        # in the same js url as above, or not.
        js_ep  = rcfg['access']['job_manager']
        with self._cache_lock:
            if js_ep in self._saga_js_cache:
                js = self._saga_js_cache[js_ep]
            else:
                js = rs.job.Service(js_ep, session=self._session)
                self._saga_js_cache[js_ep] = js

        self._log.debug('jdp: %s', pprint.pformat(jdp.as_dict()))
        job = js.create_job(jdp)
        job.run()

        # check for submission errors (startup e rrors won't be caught)
        if job.state == rs.FAILED:
            self._log.error('%s: %s : %s : %s', 
                            job.id, job.state, job.stderr, job.stdout)
            raise RuntimeError ("SAGA Job state is FAILED. (%s)" % jdp.name)


        # FIXME: update the right pilot
        with self._pilots_lock:

            self._pilots[pid] = dict()
            self._pilots[pid]['pilot'] = pilot
            self._pilots[pid]['job']   = job

        # make sure we watch that pilot
        with self._check_lock:
            self._checking.append(pid)

        self._prof.prof('submission_stop', uid=pid)


    # --------------------------------------------------------------------------
    #
    def _prepare_pilot(self, rcfg, acfg, pilot): 
        '''
        prepare job descriptions for bridge startup and pilot submission,  and
        file transfer directives for pilot startup.
        '''

        sid = self._session.uid
        pid = pilot["uid"]

      # # ----------------------------------------------------------------------
      # # the rcfg can contain keys with string expansion placeholders where
      # # values from the pilot description need filling in.  A prominent
      # # example is `%(pd.project)s`, where the pilot description's `PROJECT`
      # # value needs to be filled in (here in lowercase).
      # # 
      # # FIXME: move into ru.config or ru.dict_mixin as `dict.expand(other)
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

        # some default values are determined at runtime
        resource   = rcfg['resource']['label']
        default_ve = '%%(resource_sandbox)s/ve.%s.%s' % (resource,
                                                         self._rp_version)

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

      # # get parameters from resource cfg, set defaults where needed
        default_queue      = rcfg['system'].get('default_queue')
        tunnel_bind_device = rcfg['system'].get('tunnel_bind_device')
        forward_tunnel     = rcfg['system'].get('forward_tunnel')
        lrms               = rcfg['system'].get('lrms')
        pre_bootstrap_0    = rcfg['system'].get('pre_bootstrap_0', [])
        pre_bootstrap_1    = rcfg['system'].get('pre_bootstrap_1', [])
        python_interpreter = rcfg['system'].get('python_interpreter')
        rp_version         = rcfg['system'].get('rp_version',   DEFAULT_RP_VERSION)
        virtenv_mode       = rcfg['system'].get('virtenv_mode', DEFAULT_VIRTENV_MODE)
        virtenv            = rcfg['system'].get('virtenv',      default_ve)
        cores_per_node     = rcfg['system'].get('cores_per_node', 0)
        gpus_per_node      = rcfg['system'].get('gpus_per_node',  0)
        python_dist        = rcfg['system'].get('python_dist')
        virtenv_dist       = rcfg['system'].get('virtenv_dist', DEFAULT_VIRTENV_DIST)
        spmd_variation     = rcfg['system'].get('spmd_variation')
        shared_filesystem  = rcfg['system'].get('shared_filesystem', True)
        stage_cacerts      = rcfg['system'].get('stage_cacerts', False)
        mandatory_args     = rcfg['system'].get('mandatory_args', [])
        saga_jd_supplement = rcfg['system'].get('saga_jd_supplement', {})

        # make sure that mandatory args are known
        for ma in mandatory_args:
            if pilot['description'].get(ma) is None:
                raise  ValueError('attribute "%s" is required for "%s"'
                                 % (ma, resource))

        # get pilot and global sandbox
        resource_sbox = self._session.get_resource_sandbox(pilot)
        session_sbox  = self._session.get_session_sandbox (pilot)
        pilot_sbox    = self._session.get_pilot_sandbox   (pilot)

        resource_sbox_p = resource_sbox.path
        session_sbox_p  = session_sbox.path
        pilot_sbox_p    = pilot_sbox.path

        pilot['resource_sandbox'] = resource_sbox_p
        pilot['pilot_sandbox']    = pilot_sbox_p

        # expand variables in virtenv string
        virtenv = virtenv % {'pilot_sandbox'   : pilot_sbox_p,
                             'session_sandbox' : session_sbox_p,
                             'resource_sandbox': resource_sbox_p}

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
        if forward_tunnel         : bootstrap_args += " -f '%s'" % forward_tunnel
        if python_interpreter:      bootstrap_args += " -i '%s'" % python_interpreter
        if tunnel_bind_device:      bootstrap_args += " -t '%s'" % tunnel_bind_device
        if cleanup:                 bootstrap_args += " -x '%s'" % cleanup

        for arg in pre_bootstrap_0:
            bootstrap_args += " -e '%s'" % arg
        for arg in pre_bootstrap_1:
            bootstrap_args += " -w '%s'" % arg

        # we'll also push the agent config into MongoDB
        pilot['acfg'] = acfg
        pilot['rcfg'] = rcfg

        jdp = rs.job.Description()  # pilot job
        jdb = rs.job.Description()  # communication bridges

        # Write agent config to a json file in pilot sandbox.
        agent_cfgf = 'agent_0.cfg'
        cfg_tmp_handle, cfg_tmp_file = tempfile.mkstemp(prefix='rp.agent_cfg.')
        os.close(cfg_tmp_handle)  # file exists now

        # FIXME: add bootstrap flag or ru_def cfg
        # 'debug': self._log.getEffectiveLevel(),

        # Convert dict to json file
        self._log.debug("Write agent cfg to '%s'.", cfg_tmp_file)
        ru.write_json(pilot, cfg_tmp_file)  # contains rcfg, acfg

        # always stage the config file
        jdp.file_transfer.append({'src' : cfg_tmp_file, 
                                  'tgt' : '%s/%s' % (pilot_sbox_p, agent_cfgf),
                                  'rem' : True})  # purge the tmp file

        # always stage the agent and client bridge info
        for name in rpc.AGENT_BRIDGES:
            fname = self._session.get_address_fname(name)
            fpath = os.path.basename(fname)
            jdp.file_transfer.append({'src' : fname, 
                                      'tgt' : '%s/%s' % (pilot_sbox_p, fpath),
                                      'rem' : False})


        # check if we have a sandbox cached for that resource.  If so, we have
        # nothing to do.  Otherwise we create the sandbox and stage the RP
        # stack etc.
        # NOTE: this will race when multiple pilot launcher instances are used
        with self._cache_lock:

            if resource not in self._sbox:

                # stage RP stack
                for sdist in sdist_paths:
                    fpath = os.path.basename(sdist)
                    jdp.file_transfer.append({'src' : sdist, 
                                              'tgt' : '%s/%s' % (session_sbox_p, fpath),
                                              'rem' : False})

                # stage the bootstrapper
                bootstrapper_path = os.path.abspath("%s/agent/%s"
                                  % (self._root_dir, BOOTSTRAP_0))
                self._log.debug("use bootstrapper %s", bootstrapper_path)

                jdp.file_transfer.append({'src' : bootstrapper_path, 
                                          'tgt' : '%s/%s' % (session_sbox_p, BOOTSTRAP_0),
                                          'rem' : False})

                # Some machines cannot run pip due to outdated CA certs.
                # For those, we also stage an updated certificate bundle
                if stage_cacerts:

                    fname = '%s/agent/cacert.pem.gz' % self._root_dir
                    fpath = os.path.basename(fname)
                    self._log.debug("use CAs %s", fpath)

                    jdp.file_transfer.append({'src' : fpath, 
                                              'tgt' : '%s/%s' % (session_sbox_p, fname),
                                              'rem' : False})

                # no need to stage everything again
                self._sbox[resource] = True


        if shared_filesystem:
            bootstrap_tgt = '%s/%s' % (session_sbox_p, BOOTSTRAP_0)
        else:
            bootstrap_tgt = '%s/%s' % ('.', BOOTSTRAP_0)

        jdp.name                  = pid
        jdp.executable            = "/bin/bash"
        jdp.arguments             = ['-l %s' % bootstrap_tgt, bootstrap_args]
        jdp.working_directory     = pilot_sbox_p
        jdp.project               = project
        jdp.output                = "bootstrap_0.out"
        jdp.error                 = "bootstrap_0.err"
        jdp.total_cpu_count       = number_cores
        jdp.total_gpu_count       = number_gpus
        jdp.processes_per_host    = cores_per_node
        jdp.spmd_variation        = spmd_variation
        jdp.wall_time_limit       = runtime
        jdp.total_physical_memory = memory
        jdp.queue                 = queue
        jdp.candidate_hosts       = candidate_hosts
        jdp.environment           = dict()

        # we set any saga_jd_supplement keys which are not already set above
        for key, val in saga_jd_supplement.iteritems():
            if not jdp[key]:
                self._log.debug('supplement %s: %s', key, val)
                jdp[key] = val

        if 'RADICAL_PILOT_PROFILE' in os.environ :
            jdp.environment['RADICAL_PILOT_PROFILE'] = 'TRUE'


        jdp.file_transfer.extend([
            'site:%s/%s > %s' % (session_sbox_p, BOOTSTRAP_0, BOOTSTRAP_0),
            'site:%s/%s > %s' % (pilot_sbox_p,   agent_cfgf, agent_cfgf),
            'site:%s/%s.log.tgz > %s.log.tgz' % (pilot_sbox_p, pid, pid),
            'site:%s/%s.log.tgz < %s.log.tgz' % (pilot_sbox_p, pid, pid)
        ])

        if 'RADICAL_PILOT_PROFILE' in os.environ:
            jdp.file_transfer.extend([
                'site:%s/%s.prof.tgz > %s.prof.tgz' % (pilot_sbox_p, pid, pid),
                'site:%s/%s.prof.tgz < %s.prof.tgz' % (pilot_sbox_p, pid, pid)
            ])

        for sdist in sdist_names:
            jdp.file_transfer.extend([
                'site:%s/%s > %s' % (session_sbox_p, sdist, sdist)
            ])

        if stage_cacerts:
            fname = 'cacert.pem.gz'
            jdp.file_transfer.extend([
                'site:%s/%s > %s' % (session_sbox_p, fname, fname)
            ])

        self._log.debug("Bootstrap cmd: %s %s", jdp.executable, jdp.arguments)

        return jdp, jdb


# ------------------------------------------------------------------------------

