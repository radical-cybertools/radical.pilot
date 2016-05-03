
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import math
import pprint
import shutil
import tempfile
import threading

import subprocess as sp

import saga                 as rs
import saga.utils.pty_shell as rsup
import radical.utils        as ru

from .... import pilot     as rp
from ...  import utils     as rpu
from ...  import states    as rps
from ...  import constants as rpc

from .base import PMGRLaunchingComponent


# ------------------------------------------------------------------------------
# local constants
DEFAULT_AGENT_SPAWNER = 'POPEN'
DEFAULT_RP_VERSION    = 'local'
DEFAULT_VIRTENV       = '%(global_sandbox)s/ve'
DEFAULT_VIRTENV_MODE  = 'update'
DEFAULT_AGENT_CONFIG  = 'default'

JOB_CHECK_INTERVAL    = 60  # seconds between runs of the job state check loop
JOB_CHECK_MAX_MISSES  =  3  # number of times to find a job missing before
                            # declaring it dead

LOCAL_SCHEME = 'file'
BOOTSTRAPPER = "bootstrap_1.sh"

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

        self.register_input(rps.PMGR_LAUNCHING_PENDING, 
                            rpc.PMGR_LAUNCHING_QUEUE, self.work)

        # we don't really have an output queue, as we pass control over the
        # pilot jobs to the resource management system (RM).

        self._pilots        = dict()             # dict for all known pilots
        self._pilots_lock   = threading.RLock()  # lock on maipulating the above
        self._checking      = list()             # pilots to check state on
        self._check_lock    = threading.RLock()  # lock on maipulating the above
        self._saga_fs_cache = dict()             # cache of saga directories
        self._saga_js_cache = dict()             # cache of saga job services
        self._sandboxes     = dict()             # cache of global sandbox URLs
        self._cache_lock    = threading.RLock()  # lock for cache

        self._mod_dir       = os.path.dirname(os.path.abspath(__file__))
        self._root_dir      = "%s/../../"   % self._mod_dir  
        self._conf_dir      = "%s/configs/" % self._root_dir 
        
        # FIXME: make interval configurable
        self.register_timed_cb(self._pilot_watcher_cb, timer=1.0)
        
        # we listen for pilot cancel commands
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._pmgr_control_cb)

        _, _, _, self._rp_sdist_name, self._rp_sdist_path = \
                ru.get_version([self._root_dir, self._mod_dir])


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        with self._cache_lock:
            for js in self._saga_js_cache.values():
                js.close()
            self._saga_js_cache.clear()


    # --------------------------------------------------------------------------
    #
    def _pmgr_control_cb(self, topic, msg):

        # wait for 'terminate' commands, but only accept those where 'src' is
        # either myself, my session, or my owner

        cmd = msg['cmd']
        arg = msg['arg']

        self._log.debug('launcher got %s', msg)

        if cmd == 'cancel_pilots':

            pmgr = arg['pmgr']
            pids = arg['uids']

            if pmgr != self._pmgr:
                # this request is not for us to enact
                return

            if not isinstance(pids, list):
                pids = [pids]

            self._log.info('received pilot_cancel command (%s)', pids)

            pilots = list()
            tc     = rs.task.Container()
            with self._pilots_lock:

                for pid in pids:

                    if pid not in self._pilots:
                        self._log.error('unknown: %s', pid)
                        raise ValueError('unknown pilot %s' % pid)

                    pilots.append(self._pilots[pid]['pilot'])
                    tc.add(self._pilots[pid]['job'])

            tc.cancel()
            tc.wait()

            for pilot in pilots:

                pid = pilot['uid']

                # we don't want the watcher checking for this pilot anymore
                with self._check_lock:
                    self._checking.remove(pid)

                # FIXME: where is my bulk?
                out, err, log  = self._get_pilot_logs(pilot)
                pilot['out']   = out
                pilot['err']   = err
                pilot['log']   = log
                pilot['state'] = rps.CANCELED

            self.advance(pilots, push=False, publish=True)


    # --------------------------------------------------------------------------
    #
    def _get_pilot_logs(self, pilot):

        s_out = None
        s_err = None
        s_log = None

        # attempt to get stdout/stderr/log.  We only expect those if pilot was
        # attempting launch at some point
        # FIXME: check state
        # FIXME: we have many different log files - should we fetch more? all?

        MAX_IO_LOGLENGTH = 10240    # 10k should be enough for anybody...

        try:
            n_out = "%s/%s/%s" % (pilot['sandbox'], self._session.uid, 'agent.out')
            f_out = rs.filesystem.File(n_out, session=self._session)
            s_out = f_out.read()[-MAX_IO_LOGLENGTH:]
            f_out.close ()
        except Exception as e:
            s_out = 'stdout is not available (%s)' % e

        try:
            n_err = "%s/%s/%s" % (pilot['sandbox'], self._session.uid, 'agent.err')
            f_err = rs.filesystem.File(n_err, session=self._session)
            s_err = f_err.read()[-MAX_IO_LOGLENGTH:]
            f_err.close ()
        except Exception as e:
            s_err = 'stderr is not available (%s)' % e

        try:
            n_log = "%s/%s/%s" % (pilot['sandbox'], self._session.uid, 'agent.log')
            f_log = rs.filesystem.File(n_log, session=self._session)
            s_log = f_log.read()[-MAX_IO_LOGLENGTH:]
            f_log.close ()
        except Exception as e:
            s_log = 'log is not available (%s)' % e

        return s_out, s_err, s_log


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

        # if none of the states is final, we have nothing to do.
        # We can't rely on the ordering of tasks and states in the task
        # container, so we hope that the task container's bulk state query lead
        # to a caching of state information, and we thus have cache hits when
        # querying the pilots individually

        pilots = list()
        with self._pilots_lock, self._check_lock:
            for pid in self._checking:
                state = self._pilots[pid]['job'].state
                if state in [rs.job.DONE, rs.job.FAILED, rs.job.CANCELED]:
                    pilots.append(self._pilots[pid]['pilot'])

        if not pilots:
            # no final pilots
            return

        for pilot in pilots:

            out, err, log = self._get_pilot_logs(pilot)
            pilot['out'] = out
            pilot['err'] = err
            pilot['log'] = log

            with self._check_lock:
                # stop monitoring this pilot
                self._checking.remove(pilot['uid'])

        self.advance(pilots, rps.CANCELED, push=False, publish=True)


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

                pilots = buckets[resource][schema]
                pids   = [p['uid'] for p in pilots]
                self._log.info("Launching pilots on %s: %s", resource, pids)
                               
                self._start_pilot_bulk(resource, schema, pilots)


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

        ft_list = list()  # files to stage
        jd_list = list()  # jobs  to submit
        for pilot in pilots:
            info = self._prepare_pilot(resource, rcfg, pilot)
            ft_list += info['ft']
            jd_list.append(info['jd'])

        # we create a fake session_sandbox with all pilot_sandboxes in /tmp, and
        # then tar it up.  Once we untar that tarball on the target machine, we
        # should have all sandboxes and all files required to bootstrap the
        # pilots
        # NOTE: on untar, there is a race between multiple launcher components
        #       within the same session toward the same target resource.
        tmp_dir  = tempfile.mkdtemp(prefix='rp_agent_tar_dir')
        tar_name = '%s.%s.tgz' % (sid, self.uid)
        tar_tgt  = '%s/%s'     % (tmp_dir, tar_name)
        tar_url  = rs.Url('file://localhost/%s' % tar_tgt)

        global_sandbox  = self._session._get_global_sandbox (pilot).path
        session_sandbox = self._session._get_session_sandbox(pilot).path
        pilot_sandbox   = self._session._get_pilot_sandbox  (pilot).path

        # FIXME: pilot_sandbox differs per pilot!

        for ft in ft_list:
            src     = os.path.abspath(ft['src'])
            tgt     = os.path.normpath(ft['tgt'])
            src_dir = os.path.dirname(src)
            tgt_dir = os.path.dirname(tgt)

            if not os.path.isdir('%s/%s' % (tmp_dir, tgt_dir)):
                os.makedirs('%s/%s' % (tmp_dir, tgt_dir))
            os.symlink(src, '%s/%s' % (tmp_dir, tgt))

        # tar.  If any command fails, this will raise.
        cmd = "cd %s && tar zchf %s *" % (tmp_dir, tar_tgt)
        self._log.debug('cmd: %s', cmd)
        out = sp.check_output(["/bin/sh", "-c", cmd], stderr=sp.STDOUT)
        self._log.debug('out: %s', out)

        # remove all files marked for removal-after-pack
        for ft in ft_list:
            if ft['rem']:
                os.unlink(ft['src'])

        fs_endpoint = rcfg['filesystem_endpoint']
        fs_url      = rs.Url(fs_endpoint)

        self._log.debug ("rs.file.Directory ('%s')" % fs_url)

        with self._cache_lock:
            if fs_url in self._saga_fs_cache:
                fs = self._saga_fs_cache[fs_url]
            else:
                fs = rs.filesystem.Directory(fs_url, session=self._session)
                self._saga_fs_cache[fs_url] = fs

        tar_rem      = rs.Url(fs_url)
        tar_rem.path = "%s/%s" % (session_sandbox, tar_name)

        fs.copy(tar_url, tar_rem, flags=rs.filesystem.CREATE_PARENTS)

        shutil.rmtree(tmp_dir)


        # we now need to untar on the target machine -- if needed use the hop
        js_ep   = rcfg['job_manager_endpoint']
        js_hop  = rcfg.get('job_manager_hop', js_ep)
        js_url  = rs.Url(js_hop)

        with self._cache_lock:
            if js_url in self._saga_js_cache:
                js_tmp = self._saga_js_cache[js_url]
            else:
                js_tmp = rs.job.Service(js_url, session=self._session)
                self._saga_js_cache[js_url] = js_tmp
        cmd = "tar zmxvf %s/%s -C /" % (session_sandbox, tar_name)
        j = js_tmp.run_job(cmd)
        j.wait()

        self._log.debug('tar cmd : %s', cmd)
        self._log.debug('tar done: %s, %s, %s', j.state, j.stdout, j.stderr)

        if js_ep == js_hop:
            # we can use the same job service for pilot submission
            js = js_tmp
        else:
            # we need a different js for actual job submission
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

            pilot = None
            for p in pilots:
                self._log.debug(' === checking job name for %s:%s:%s', j.id, j.name, j.get_name())
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

        self.advance(pilots, rps.PMGR_ACTIVE_PENDING, push=False, publish=True)


    # --------------------------------------------------------------------------
    #
    def _prepare_pilot(self, resource, rcfg, pilot):

        pid = pilot["uid"]
        ret = {'ft' : list(),
               'jd' : None  }

        # ------------------------------------------------------------------
        # Database connection parameters
        sid           = self._session.uid
        database_url  = self._session.dburl

        # ------------------------------------------------------------------
        # pilot description and resource configuration
        cores           = pilot['description']['cores']
        runtime         = pilot['description']['runtime']
        queue           = pilot['description']['queue']
        project         = pilot['description']['project']
        cleanup         = pilot['description']['cleanup']
        memory          = pilot['description']['memory']
        candidate_hosts = pilot['description']['candidate_hosts']

        # ------------------------------------------------------------------
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
        pre_bootstrap_1         = rcfg.get('pre_bootstrap_1', [])
        pre_bootstrap_2         = rcfg.get('pre_bootstrap_2', [])
        python_interpreter      = rcfg.get('python_interpreter')
        task_launch_method      = rcfg.get('task_launch_method')
        rp_version              = rcfg.get('rp_version',          DEFAULT_RP_VERSION)
        virtenv_mode            = rcfg.get('virtenv_mode',        DEFAULT_VIRTENV_MODE)
        virtenv                 = rcfg.get('virtenv',             DEFAULT_VIRTENV)
        cores_per_node          = rcfg.get('cores_per_node', 0)
        health_check            = rcfg.get('health_check', True)
        python_dist             = rcfg.get('python_dist')
        spmd_variation          = rcfg.get('spmd_variation')
        shared_filesystem       = rcfg.get('shared_filesystem', True)
        stage_cacerts           = rcfg.get ('stage_cacerts', False)


        # get pilot and global sandbox
        global_sandbox   = self._session._get_global_sandbox (pilot).path
        session_sandbox  = self._session._get_session_sandbox(pilot).path
        pilot_sandbox    = self._session._get_pilot_sandbox  (pilot).path
        pilot['sandbox'] = str(self._session._get_pilot_sandbox(pilot))

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
            # nothing to do
            agent_cfg = agent_config
            pass

        elif isinstance(agent_config, basestring):
            try:
                if os.path.exists(agent_config):
                    # try to open as file name
                    self._log.info("Read agent config file: %s" % agent_config)
                    agent_cfg = ru.read_json(agent_config)
                else:
                    # otherwise interpret as a config name
                    # FIXME: load in session just like resource
                    #        configs, including user level overloads
                    agent_cfg_file = os.path.join(self._conf_dir, "agent_%s.json" % agent_config)
                    self._log.info("Read agent config file: %s" % agent_cfg_file)
                    agent_cfg = ru.read_json(agent_cfg_file)
            except Exception as e:
                self._log.exception("Error reading agent config file: %s" % e)
                raise

        else:
            # we can't handle this type
            raise TypeError('agent config must be string (filename) or dict')

        # expand variables in virtenv string
        virtenv = virtenv % {'pilot_sandbox'   :   pilot_sandbox,
                             'session_sandbox' : session_sandbox,
                             'global_sandbox'  :  global_sandbox}

        # Check for deprecated global_virtenv
        if 'global_virtenv' in rcfg:
            raise RuntimeError("'global_virtenv' is deprecated (%s)" % resource)

        # Create a host:port string for use by the bootstrap_1.
        db_url = rs.Url(agent_dburl)
        if db_url.port:
            db_hostport = "%s:%d" % (db_url.host, db_url.port)
        else:
            db_hostport = "%s:%d" % (db_url.host, 27017) # mongodb default

        # ------------------------------------------------------------------
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


        # ------------------------------------------------------------------
        # sanity checks
        if not agent_spawner      : raise RuntimeError("missing agent spawner")
        if not agent_scheduler    : raise RuntimeError("missing agent scheduler")
        if not lrms               : raise RuntimeError("missing LRMS")
        if not agent_launch_method: raise RuntimeError("missing agentlaunch method")
        if not task_launch_method : raise RuntimeError("missing task launch method")

        # massage some values
        if not queue :
            queue = default_queue

        if  cleanup and isinstance (cleanup, bool) :
            cleanup = 'luve'    #  l : log files
                                #  u : unit work dirs
                                #  v : virtualenv
                                #  e : everything (== pilot sandbox)
                                #
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

        # cores can contain a partitioning specification like # 32:32:64 
        # (3 partitions with 32, 32 and 64 cores each), so we need to
        # derive the actual core count.
        total_cores = 0
        partitions  = 0

        # if cores_per_node is set (!= None), then we need to allocation full
        # nodes, and thus round up.  We need to do this for each partition, as
        # we currently only partition at node boundaries.  This may change the
        # overall core count, so we update 'cores', too.
        if cores_per_node:
            cores_per_node = int(cores_per_node)
            new_cores      = ''
            for c in cores.split(':'):
                part_cores   = int(cores_per_node
                             * math.ceil(float(c)/cores_per_node))
                total_cores += part_cores
                new_cores   += '%d:' % part_cores
                partitions  += 1
            cores = new_cores[:-1]  # remove trailing ':'
        else:
            # otherwise, total cores is just the sum of the given partitions.
            for c in cores.split(':'):
                total_cores += int(c)
                partitions  += 1

        # set mandatory args
        bootstrap_args  = ""
        bootstrap_args += " -d '%s'" % ':'.join(sdist_names)
        bootstrap_args += " -p '%s'" % pid
        bootstrap_args += " -s '%s'" % sid
        bootstrap_args += " -m '%s'" % virtenv_mode
        bootstrap_args += " -n '%d'" % partitions
        bootstrap_args += " -r '%s'" % rp_version
        bootstrap_args += " -b '%s'" % python_dist
        bootstrap_args += " -v '%s'" % virtenv

        # set optional args
        if lrms == "CCM":           bootstrap_args += " -c"
        if forward_tunnel_endpoint: bootstrap_args += " -f '%s'" % forward_tunnel_endpoint
        if forward_tunnel_endpoint: bootstrap_args += " -h '%s'" % db_hostport
        if python_interpreter:      bootstrap_args += " -i '%s'" % python_interpreter
        if tunnel_bind_device:      bootstrap_args += " -t '%s'" % tunnel_bind_device
        if cleanup:                 bootstrap_args += " -x '%s'" % cleanup

        for arg in pre_bootstrap_1:
            bootstrap_args += " -e '%s'" % arg
        for arg in pre_bootstrap_2:
            bootstrap_args += " -w '%s'" % arg

        # complete agent configuration
        # NOTE: the agent config is really based on our own config, with 
        #       agent specific settings merged in

        agent_base_cfg = copy.deepcopy(self._cfg)
        del(agent_base_cfg['bridges'])    # agent needs separate bridges
        del(agent_base_cfg['components']) # agent needs separate components
        del(agent_base_cfg['number'])     # agent counts differently
        del(agent_base_cfg['heart'])      # agent needs separate heartbeat

        ru.dict_merge(agent_cfg, agent_base_cfg, ru.PRESERVE)

        # depending on the number of partitions, we want to massage the agent
        # config a little.  At the moment, we simply consider the agent configs
        # to apply identically to each partition, thus we replicate all
        # sub-agent config session for all partitions
        layout = copy.deepcopy(agent_cfg['agent_layout'])
        agent_cfg['agent_layout'] = dict()
        for sub_agent in layout:
            agent_id = int(sub_agent.split('_', 1)[1])
            for p in range(partitions):
                agent_name = 'agent_%d_%s' % (p, agent_id)
                agent_cfg['agent_layout'][agent_name] = layout[sub_agent]
                agent_cfg['agent_layout'][agent_name]['partition'] = p

        agent_cfg['cores']              = cores
        agent_cfg['debug']              = os.environ.get('RADICAL_PILOT_AGENT_VERBOSE', 
                                                         self._log.getEffectiveLevel())
        agent_cfg['lrms']               = lrms
        agent_cfg['spawner']            = agent_spawner
        agent_cfg['scheduler']          = agent_scheduler
        agent_cfg['runtime']            = runtime
        agent_cfg['pilot_id']           = pid
        agent_cfg['logdir']             = pilot_sandbox
        agent_cfg['pilot_sandbox']      = pilot_sandbox
        agent_cfg['session_sandbox']    = session_sandbox
        agent_cfg['global_sandbox']     = global_sandbox
        agent_cfg['agent_launch_method']= agent_launch_method
        agent_cfg['task_launch_method'] = task_launch_method
        agent_cfg['mpi_launch_method']  = mpi_launch_method

        # we'll also push the agent config into MongoDB
        pilot['cfg'] = agent_cfg

        # ------------------------------------------------------------------
        # Write agent config dict to a json file in pilot sandbox.

        agent_cfg_name = 'agent.cfg'
        cfg_tmp_handle, cfg_tmp_file = tempfile.mkstemp(prefix='rp.agent_cfg.')
        os.close(cfg_tmp_handle)  # file exists now

        # Convert dict to json file
        self._log.debug("Write agent cfg to '%s'.", cfg_tmp_file)
        ru.write_json(agent_cfg, cfg_tmp_file)

        ret['ft'].append({'src' : cfg_tmp_file, 
                          'tgt' : '%s/%s' % (pilot_sandbox, agent_cfg_name),
                          'rem' : True})  # purge the tmp file after packing

        # check if we have a sandbox cached for that resource.  If so, we have
        # nothing to do.  Otherwise we create the sandbox and stage the RP
        # stack etc.
        # NOTE: this will race when multiple pilot launcher instances are used!
        with self._cache_lock:

            if not resource in self._sandboxes:

                for sdist in sdist_paths:
                    base = os.path.basename(sdist)
                    ret['ft'].append({'src' : sdist, 
                                      'tgt' : '%s/%s' % (session_sandbox, base),
                                      'rem' : False})

                # Copy the bootstrap shell script.
                bootstrapper_path = os.path.abspath("%s/agent/%s" \
                        % (self._root_dir, BOOTSTRAPPER))
                self._log.debug("use bootstrapper %s", bootstrapper_path)

                ret['ft'].append({'src' : bootstrapper_path, 
                                  'tgt' : '%s/%s' % (session_sandbox, BOOTSTRAPPER),
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


        # ------------------------------------------------------------------
        # Create SAGA Job description and submit the pilot job

        jd = rs.job.Description()

        if shared_filesystem:
            bootstrap_tgt = '%s/%s' % (session_sandbox, BOOTSTRAPPER)
        else:
            bootstrap_tgt = '%s/%s' % ('.', BOOTSTRAPPER)

        jd.name                  = pid
        self._log.debug(' === set jd name to %s'% pid)
        jd.executable            = "/bin/bash"
        jd.arguments             = ['-l %s' % bootstrap_tgt, bootstrap_args]
        jd.working_directory     = pilot_sandbox
        jd.project               = project
        jd.output                = "bootstrap_1.out"
        jd.error                 = "bootstrap_1.err"
        jd.total_cpu_count       = total_cores
        jd.processes_per_host    = cores_per_node
        jd.spmd_variation        = spmd_variation
        jd.wall_time_limit       = runtime
        jd.total_physical_memory = memory
        jd.queue                 = queue
        jd.candidate_hosts       = candidate_hosts
        jd.environment           = dict()

        if 'RADICAL_PILOT_PROFILE' in os.environ :
            jd.environment['RADICAL_PILOT_PROFILE'] = 'TRUE'

        # for condor backends and the like which do not have shared FSs, we add
        # additional staging directives so that the backend system binds the
        # files from the session and pilot sandboxes to the pilot job.
        jd.file_transfer = list()
        if not shared_filesystem:

            jd.file_transfer.extend([
                'site:%s/%s > %s' % (session_sandbox, BOOTSTRAPPER,   BOOTSTRAPPER),
                'site:%s/%s > %s' % (pilot_sandbox,   agent_cfg_name, agent_cfg_name),
                'site:%s/%s.log.tgz < %s.log.tgz' % (session_sandbox, pid, pid)
            ])

            for sdist in sdist_names:
                jd.file_transfer.append(
                    'site:%s/%s > %s' % (session_sandbox, sdist, sdist))

            if stage_cacerts:
                jd.file_transfer.append(
                    'site:%s/%s > %s' % (session_sandbox, cc_name, cc_name)
                )

            if 'RADICAL_PILOT_PROFILE' in os.environ:
                jd.file_transfer.append(
                    'site:%s/%s.prof.tgz < %s.prof.tgz' % (session_sandbox, pid, pid)
                )

        self._log.debug("Bootstrap command line: %s %s" % (jd.executable, jd.arguments))

        ret['jd'] = jd
        return ret


# ------------------------------------------------------------------------------

