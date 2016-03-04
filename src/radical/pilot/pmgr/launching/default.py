
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import math
import pprint
import tempfile
import threading

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
DEFAULT_AGENT_TYPE    = 'multicore'
DEFAULT_AGENT_SPAWNER = 'POPEN'
DEFAULT_RP_VERSION    = 'local'
DEFAULT_VIRTENV       = '%(global_sandbox)s/ve'
DEFAULT_VIRTENV_MODE  = 'update'
DEFAULT_AGENT_CONFIG  = 'default'

JOB_CHECK_INTERVAL    = 60  # seconds between runs of the job state check loop
JOB_CHECK_MAX_MISSES  =  3  # number of times to find a job missing before
                            # declaring it dead

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

        self.declare_input(rps.PMGR_LAUNCHING_PENDING, 
                           rpc.PMGR_LAUNCHING_QUEUE, self.work)

        # we don't really have an output queue, as we pass control over the
        # pilot jobs to the resource management system (RM).

        self._saga_js       = dict()             # cache of saga.job.Services
        self._pilots        = dict()             # dict for all known pilots
        self._tocheck       = list()             # pilots run state checks on
        self._missing       = list()             # for failed state checks
        self._pilots_lock   = threading.RLock()  # lock on maipulating the above
        self._saga_js_cache = dict()             # cache of saga job services
        self._sandbox_cache = dict()             # cache of global sandbox URLs
        self._cache_lock    = threading.RLock()  # lock for cache

        self._mod_dir       = os.path.dirname(os.path.abspath(__file__))
        self._root_dir      = "%s/../../"   % self._mod_dir  
        self._conf_dir      = "%s/configs/" % self._root_dir 
        
        # FIXME: make interval configurable
        self.declare_idle_cb(self._pilot_watcher_cb, timeout=10.0)
        
        # we listen for pilot cancel commands
        self.declare_subscriber('control', rpc.CONTROL_PUBSUB, self._pmgr_control_cb)

        _, _, _, self._rp_sdist_name, self._rp_sdist_path = \
                ru.get_version([self._root_dir, self._mod_dir])


    # --------------------------------------------------------------------------
    #
    def _pmgr_control_cb(self, topic, msg):

        # wait for 'terminate' commands, but only accept those where 'src' is
        # either myself, my session, or my owner

        print 'pmgr_control got     %s' % msg

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_pilots':
            uids = arg['uids']

            print 'pmgr_control handles %s' % msg
            with self._pilots_lock:

                print 'pmgr_control handles %s now' % msg
                pprint.pprint(self._pilots)

                if not isinstance(uids, list):
                    uids = [uids]

                self._log.info('received pilot_cancel command (%s)', uids)

                saga_jobs = list()
                for uid in uids:
                    if uid not in self._pilots:
                        raise 'cannot cancel pilot %s: unknown' % uid

                    saga_pid  = self._pilots[uid]['_saga_pid']   
                    js_url    = rs.Url(self._pilots[uid]['_saga_js_url'])

                    with self._cache_lock:
                        if js_url in self._saga_js_cache:
                            js = self._saga_js_cache[js_url]
                        else :
                            js = rs.job.Service(js_url, session=self._session)
                            self._saga_js_cache[js_url] = js

                    saga_job = js.get_job(saga_pid)
                    saga_job.cancel()
                    saga_jobs.append(saga_job)
                    print 'canceled %s [%s]' % (uid, saga_pid)
                    
                for saga_job in saga_jobs:
                    saga_job.wait()
                    print 'final    %s [%s]' % (uid, saga_pid)

        else:
            print 'pmgr_control ignores %s' % msg

            


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
            n_out = "%s/%s" % (pilot['sandbox'], 'agent_0.out')
            f_out = rs.filesystem.File(n_out, session=self._session)
            s_out = f_out.read()[-MAX_IO_LOGLENGTH:]
            f_out.close ()
        except Exception as e:
            s_out = 'stdout is not available (%s)' % e

        try:
            n_err = "%s/%s" % (pilot['sandbox'], 'agent_0.err')
            f_err = rs.filesystem.File(n_err, session=self._session)
            s_err = f_err.read()[-MAX_IO_LOGLENGTH:]
            f_err.close ()
        except Exception as e:
            s_err = 'stderr is not available (%s)' % e

        try:
            n_log = "%s/%s" % (pilot['sandbox'], 'agent_0.log')
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
        #       This implies that we need to communicate 'final_cause'
        
        # we don't want to lock our members all the time.  For that reason we
        # use a copy of the pilots_tocheck list and iterate over that, and only
        # lock other members when they are manipulated.

        with self._pilots_lock:
            checklist = self._tocheck[:]  # deep copy

        for pid,saga_pid in checklist:

            pilot_failed = False
            pilot_done   = False
            log_message  = ""

            print "health check for %s [%s]" % (pid, saga_pid)
            self._log.info("health check for %s [%s]", pid, saga_pid)

            try:
                js_url = saga_pid.split("]-[")[0][1:]
                js     = self._saga_js.get(js_url)

                if not js:
                    js = rs.job.Service(js_url, session=self._session)
                    self._saga_js[js_url] = js

                saga_job = js.get_job(saga_pid)
                print "SAGA job state for %s: %s." % (pid, saga_job.state)
                self._log.debug("SAGA job state for %s: %s.", pid, saga_job.state)

                if  saga_job.state in [rs.job.FAILED, rs.job.CANCELED]:
                    pilot_failed = True

                if  saga_job.state in [rs.job.DONE]:
                    pilot_done = True


            except Exception as e:

                if pid not in self._missing: self._missing[pid]  = 1
                else                       : self._missing[pid] += 1

                self._log.exception ('could not reconnect to pilot (%s)', e)

                if  self._missing[pid] >= JOB_CHECK_MAX_MISSES:
                    self._log.debug('giving up (%s attempts)', self._missing[pid])
                    pilot_failed = True


            if  pilot_failed:
                print 'failed'
                with self._pilots_lock:
                    pilot = self._pilots[pid]
                    out, err, log = self._get_pilot_logs(pilot)
                    pilot['out'] = out
                    pilot['err'] = err
                    pilot['log'] = log
                    self._log.warn('pilot %s is dead', pid)
                    self.advance(pilot, rps.FAILED, push=False, publish=True)

            elif pilot_done:
                print 'done'
                with self._pilots_lock:
                    out, err, log = self._get_pilot_logs(pilot)
                    pilot['out'] = out
                    pilot['err'] = err
                    pilot['log'] = log
                    self._log.info('pilot %s is done', pid)
                    self.advance(pilot, rps.DONE, push=False, publish=True)

            # no need to watch final pilots anymore...
            if pilot_done or pilot_failed:
                with self._pilots_lock:
                    self._tocheck.remove([pid,saga_pid])


    # --------------------------------------------------------------------------
    #
    def work(self, pilot):

        self.advance(pilot, rps.PMGR_LAUNCHING, publish=True, push=False)
        self._log.info('handle %s' % pilot['uid'])

        # NOTE: we use a rather conservative grained lock here which may apply
        #       for quite some time.  The upside is is the self._pilots dict
        #       always contains pilots which have a saga_pid, so that they can
        #       be canceled.
        with self._pilots_lock:

            pid = pilot["uid"]

            self._log.info("Launching ComputePilot %s" % pid)

            # ------------------------------------------------------------------
            # Database connection parameters
            session_id    = self._session.uid
            database_url  = self._session.dburl

            # ------------------------------------------------------------------
            # pilot description and resource configuration
            number_cores    = pilot['description']['cores']
            runtime         = pilot['description']['runtime']
            queue           = pilot['description']['queue']
            project         = pilot['description']['project']
            cleanup         = pilot['description']['cleanup']
            resource        = pilot['description']['resource']
            schema          = pilot['description']['access_schema']
            memory          = pilot['description']['memory']
            candidate_hosts = pilot['description']['candidate_hosts']

            # get pilot and global sandbox
            global_sandbox   = self._session._get_global_sandbox(pilot)
            pilot_sandbox    = self._session._get_pilot_sandbox(pilot)
            pilot['sandbox'] = pilot_sandbox

            # ------------------------------------------------------------------
            # get parameters from resource cfg, set defaults where needed
            rcfg = self._session.get_resource_config(resource, schema)

            agent_launch_method     = rcfg.get ('agent_launch_method')
            agent_dburl             = rcfg.get ('agent_mongodb_endpoint', database_url)
            agent_spawner           = rcfg.get ('agent_spawner',       DEFAULT_AGENT_SPAWNER)
            agent_type              = rcfg.get ('agent_type',          DEFAULT_AGENT_TYPE)
            rc_agent_config         = rcfg.get ('agent_config',        DEFAULT_AGENT_CONFIG)
            agent_scheduler         = rcfg.get ('agent_scheduler')
            tunnel_bind_device      = rcfg.get ('tunnel_bind_device')
            default_queue           = rcfg.get ('default_queue')
            forward_tunnel_endpoint = rcfg.get ('forward_tunnel_endpoint')
            js_endpoint             = rcfg.get ('job_manager_endpoint')
            lrms                    = rcfg.get ('lrms')
            mpi_launch_method       = rcfg.get ('mpi_launch_method')
            pre_bootstrap_1         = rcfg.get ('pre_bootstrap_1', [])
            pre_bootstrap_2         = rcfg.get ('pre_bootstrap_2', [])
            python_interpreter      = rcfg.get ('python_interpreter')
            spmd_variation          = rcfg.get ('spmd_variation')
            task_launch_method      = rcfg.get ('task_launch_method')
            rp_version              = rcfg.get ('rp_version',          DEFAULT_RP_VERSION)
            virtenv_mode            = rcfg.get ('virtenv_mode',        DEFAULT_VIRTENV_MODE)
            virtenv                 = rcfg.get ('virtenv',             DEFAULT_VIRTENV)
            stage_cacerts           = rcfg.get ('stage_cacerts',       'False')
            cores_per_node          = rcfg.get ('cores_per_node')
            shared_filesystem       = rcfg.get ('shared_filesystem', True)
            health_check            = rcfg.get ('health_check', True)
            python_dist             = rcfg.get ('python_dist')


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

            # TODO: use booleans all the way?
            if stage_cacerts.lower() == 'true':
                stage_cacerts = True
            else:
                stage_cacerts = False

            # expand variables in virtenv string
            virtenv = virtenv % {'pilot_sandbox' : rs.Url(pilot_sandbox).path,
                                 'global_sandbox': rs.Url(global_sandbox).path }

            # Check for deprecated global_virtenv
            if 'global_virtenv' in rcfg:
                raise RuntimeError("'global_virtenv' is deprecated (%s)" % res)

            # Create a host:port string for use by the bootstrap_1.
            db_url = rs.Url(agent_dburl)
            if db_url.port:
                db_hostport = "%s:%d" % (db_url.host, db_url.port)
            else:
                db_hostport = "%s:%d" % (db_url.host, 27017) # mongodb default

            # Open the remote sandbox
            # TODO: make conditional on shared_fs?
            sandbox_tgt = rs.filesystem.Directory(pilot_sandbox,
                                                  session=self._session,
                                                  flags=rs.filesystem.CREATE_PARENTS)
            LOCAL_SCHEME = 'file'

            # ------------------------------------------------------------------
            # Copy the bootstrap shell script.
            # This also creates the sandbox.
            BOOTSTRAPPER_SCRIPT = "bootstrap_1.sh"
            bootstrapper_path   = os.path.abspath("%s/bootstrapper/%s" \
                    % (self._root_dir, BOOTSTRAPPER_SCRIPT))

            bs_script_url = rs.Url("%s://localhost%s" % (LOCAL_SCHEME, bootstrapper_path))

            self._log.debug("Using bootstrapper %s", bootstrapper_path)
            self._log.debug("Copy bootstrapper '%s' to %s.", bs_script_url, sandbox_tgt)

            if shared_filesystem:
                sandbox_tgt.copy(bs_script_url, BOOTSTRAPPER_SCRIPT)

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
                not rp_version in ['installed', 'local', 'debug']:
                raise ValueError("invalid rp_version '%s'" % rp_version)

            stage_sdist=True
            if rp_version in ['installed', 'release']:
                stage_sdist = False

            if rp_version.startswith('@'):
                stage_sdist = False
                rp_version  = rp_version[1:]  # strip '@'


            # ------------------------------------------------------------------
            # Copy the rp sdist if needed.  We actually also stage
            # the sdists for radical.utils and radical.saga, so that
            # we have the complete stack to install...
            if stage_sdist:

                for sdist_path in [ru.sdist_path, rs.sdist_path, self._rp_sdist_path]:

                    sdist_url = rs.Url("%s://localhost%s" % (LOCAL_SCHEME, sdist_path))
                    if shared_filesystem:
                        self._log.debug("Copy sdist '%s' to '%s'", sdist_url, pilot_sandbox)
                        sandbox_tgt.copy(sdist_url, os.path.basename(str(sdist_url)))


            # ------------------------------------------------------------------
            # Some machines cannot run pip due to outdated CA certs.
            # For those, we also stage an updated certificate bundle
            if stage_cacerts:
                cc_path = os.path.abspath("%s/bootstrapper/%s" \
                        % (self._root_dir, 'cacert.pem.gz'))

                cc_url= rs.Url("%s://localhost/%s" % (LOCAL_SCHEME, cc_path))
                if shared_filesystem:
                    self._log.debug("Copy CAs '%s' to '%s'", cc_url, pilot_sandbox)
                    sandbox_tgt.copy(cc_url, os.path.basename(str(cc_url)))


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

            sdists = ':'.join([ru.sdist_name, rs.sdist_name, self._rp_sdist_name])

            # if cores_per_node is set (!= None), then we need to
            # allocation full nodes, and thus round up
            if cores_per_node:
                cores_per_node = int(cores_per_node)
                number_cores = int(cores_per_node
                        * math.ceil(float(number_cores)/cores_per_node))

            # set mandatory args
            bootstrap_args  = ""
            bootstrap_args += " -d '%s'" % sdists
            bootstrap_args += " -m '%s'" % virtenv_mode
            bootstrap_args += " -p '%s'" % pid
            bootstrap_args += " -r '%s'" % rp_version
            bootstrap_args += " -s '%s'" % session_id
            bootstrap_args += " -v '%s'" % virtenv
            bootstrap_args += " -b '%s'" % python_dist

            # set optional args
            if agent_type:              bootstrap_args += " -a '%s'" % agent_type
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

            # set some agent configuration
            agent_cfg['cores']              = number_cores
            agent_cfg['debug']              = os.environ.get('RADICAL_PILOT_AGENT_VERBOSE', 
                                                             self._log.getEffectiveLevel())
            agent_cfg['mongodb_url']        = str(agent_dburl)
            agent_cfg['lrms']               = lrms
            agent_cfg['spawner']            = agent_spawner
            agent_cfg['scheduler']          = agent_scheduler
            agent_cfg['runtime']            = runtime
            agent_cfg['pilot_id']           = pid
            agent_cfg['session_id']         = session_id
            agent_cfg['pilot_sandbox']      = pilot_sandbox
            agent_cfg['global_sandbox']     = global_sandbox
            agent_cfg['agent_launch_method']= agent_launch_method
            agent_cfg['task_launch_method'] = task_launch_method
            if mpi_launch_method:
                agent_cfg['mpi_launch_method']  = mpi_launch_method
            if cores_per_node:
                agent_cfg['cores_per_node'] = cores_per_node

            # ------------------------------------------------------------------
            # Write agent config dict to a json file in pilot sandbox.

            cfg_tmp_dir = tempfile.mkdtemp(prefix='rp_agent_cfg_dir')
            agent_cfg_name = 'agent_0.cfg'
            cfg_tmp_file = os.path.join(cfg_tmp_dir, agent_cfg_name)
            cfg_tmp_handle = os.open(cfg_tmp_file, os.O_WRONLY|os.O_CREAT)

            # Convert dict to json file
            self._log.debug("Write agent cfg to '%s'.", cfg_tmp_file)
            ru.write_json(agent_cfg, cfg_tmp_file)

            cf_url = rs.Url("%s://localhost%s" % (LOCAL_SCHEME, cfg_tmp_file))
            if shared_filesystem:
                self._log.debug("Copy agent cfg '%s' to '%s'", cf_url, pilot_sandbox)
                sandbox_tgt.copy(cf_url, agent_cfg_name)

            # Close agent config file
            os.close(cfg_tmp_handle)

            # ------------------------------------------------------------------
            # Done with all transfers to pilot sandbox, close handle
            sandbox_tgt.close()

            # ------------------------------------------------------------------
            # now that the scripts are in place and configured, 
            # we can launch the agent
            js_url = rs.Url(js_endpoint)
            self._log.debug ("rs.job.Service ('%s')" % js_url)
            with self._cache_lock:
                if js_url in self._saga_js_cache:
                    js = self._saga_js_cache[js_url]
                else :
                    js = rs.job.Service(js_url, session=self._session)
                    self._saga_js_cache[js_url] = js


            # ------------------------------------------------------------------
            # Create SAGA Job description and submit the pilot job

            jd = rs.job.Description()

            jd.executable            = "/bin/bash"
            jd.arguments             = ["-l %s" % BOOTSTRAPPER_SCRIPT, bootstrap_args]
            jd.working_directory     = rs.Url(pilot_sandbox).path
            jd.project               = project
            jd.output                = "bootstrap_1.out"
            jd.error                 = "bootstrap_1.err"
            jd.total_cpu_count       = number_cores
            jd.processes_per_host    = cores_per_node
            jd.wall_time_limit       = runtime
            jd.total_physical_memory = memory
            jd.queue                 = queue
            jd.candidate_hosts       = candidate_hosts
            jd.environment           = dict()

            # TODO: not all files might be required, this also needs to be made conditional
            if not shared_filesystem:
                jd.file_transfer = [
                  # '%s > %s' % (bootstrapper_path, os.path.basename(bootstrapper_path)),
                    '%s > %s' % (bootstrapper_path, os.path.join(jd.working_directory,
                                 'input', os.path.basename(bootstrapper_path))),
                    '%s > %s' % (cfg_tmp_file, os.path.join(jd.working_directory, 
                                 'input', agent_cfg_name)),
                  # '%s < %s' % ('agent.log', os.path.join(jd.working_directory, 'agent.log')),
                  # '%s < %s' % (os.path.join(jd.working_directory, 'agent.log'), 'agent.log'),
                  # '%s < %s' % ('agent.log', 'agent.log'),
                  # '%s < %s' % (os.path.join(jd.working_directory, 'STDOUT'), 'unit.000000/STDOUT'),
                  # '%s < %s' % (os.path.join(jd.working_directory, 'unit.000000/STDERR'), 'STDERR')
                  # '%s < %s' % ('unit.000000/STDERR', 'unit.000000/STDERR')
                    
                  #  TODO: This needs to go into a per session directory on the submit node
                    '%s < %s' % ('pilot.0000.log.tgz', 'pilot.0000.log.tgz')
                ]

                if stage_sdist:
                    jd.file_transfer.extend([
                      # '%s > %s' % (self._rp_sdist_path, os.path.basename(self._rp_sdist_path)),
                        '%s > %s' % (self._rp_sdist_path, os.path.join(jd.working_directory,
                                     'input', os.path.basename(self._rp_sdist_path))),
                      # '%s > %s' % (rs.sdist_path, os.path.basename(rs.sdist_path)),
                        '%s > %s' % (rs.sdist_path, os.path.join(jd.working_directory, 
                                     'input', os.path.basename(rs.sdist_path))),
                      # '%s > %s' % (ru.sdist_path, os.path.basename(ru.sdist_path)),
                        '%s > %s' % (ru.sdist_path, os.path.join(jd.working_directory, 
                                     'input', os.path.basename(ru.sdist_path)))
                    ])

                if stage_cacerts:
                    jd.file_transfer.append(
                        '%s > %s' % (cc_path, os.path.join(jd.working_directory,
                                     'input', os.path.basename(cc_path)))
                    )

                if 'RADICAL_PILOT_PROFILE' in os.environ :
                    # TODO: This needs to go into a per session directory on 
                    #       the submit node
                    jd.file_transfer.append(
                        '%s < %s' % ('pilot.0000.prof.tgz', 'pilot.0000.prof.tgz')
                    )

            # Set the SPMD variation only if required
            if spmd_variation:
                jd.spmd_variation = spmd_variation

            if 'RADICAL_PILOT_PROFILE' in os.environ :
                jd.environment['RADICAL_PILOT_PROFILE'] = 'TRUE'

            self._log.debug("Bootstrap command line: %s %s" % (jd.executable, jd.arguments))
            self._log.debug("Submit SAGA job with: '%s'", str(jd.as_dict()))

            saga_job = js.create_job(jd)
            saga_job.run()

            # Clean up agent config file after submission
            os.unlink(cfg_tmp_file)

            # do a quick error check
            if saga_job.state == rs.FAILED:
                raise RuntimeError ("SAGA Job state is FAILED.")

            saga_pid = saga_job.id
            self._pilots[pid] = pilot
            self._pilots[pid]['_saga_pid']    = saga_pid
            self._pilots[pid]['_saga_js_url'] = str(js_url)
            self._log.debug("SAGA job id: %s", saga_pid)

            #
            # ------------------------------------------------------------------

            # make sure we watch that pilot
            self._tocheck.append([pid, saga_pid])


            # Update the Pilot's state to 'PMGR_ACTIVE_PENDING' if SAGA job
            # submission was successful.  Since the pilot leaves the scope of
            # the PMGR for the time being, we update the complete DB document
            pilot['cfg']  = agent_cfg
            pilot['$all'] = True
            self.advance(pilot, rps.PMGR_ACTIVE_PENDING, push=False, publish=True)

        self._log.debug("work done")

# ------------------------------------------------------------------------------

