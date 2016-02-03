"""
.. module:: radical.pilot.controller.pilot_launcher_worker
.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import copy
import math
import time
import traceback
import threading
import tempfile

import saga
import radical.utils as ru

from ..states    import *
from ..utils     import logger
from ..utils     import timestamp
from ..context   import Context
from ..logentry  import Logentry

pwd = os.path.dirname(__file__)
root = "%s/../" % pwd
_, _, _, rp_sdist_name, rp_sdist_path = ru.get_version([root, pwd])

IDLE_TIMER           =  1  # seconds to sleep if notthing to do
JOB_CHECK_INTERVAL   = 60  # seconds between runs of the job state check loop
JOB_CHECK_MAX_MISSES =  3  # number of times to find a job missing before
                           # declaring it dead

DEFAULT_AGENT_TYPE    = 'multicore'
DEFAULT_AGENT_SPAWNER = 'POPEN'
DEFAULT_RP_VERSION    = 'local'
DEFAULT_VIRTENV       = '%(global_sandbox)s/ve'
DEFAULT_VIRTENV_MODE  = 'update'
DEFAULT_AGENT_CONFIG  = 'default'
DEFAULT_PYTHON_DIST   = 'default'

# ----------------------------------------------------------------------------
#
class PilotLauncherWorker(threading.Thread):
    """PilotLauncherWorker handles bootstrapping and launching of
       the pilot agents.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, session, pilot_manager_id,
                 shared_worker_data, number=None):
        """Creates a new pilot launcher background process.
        """
        self._session = session

        # threading stuff
        threading.Thread.__init__(self)

        self.pilot_manager_id   = pilot_manager_id
        self.name               = "PilotLauncherWorker-%s" % str(number)
        self.missing_pilots     = dict()
        self._shared_worker_data = shared_worker_data

        # disable event for launcher functionality (not state check
        # functionality)
        self._disabled = threading.Event()
        self._disabled.clear()

        # Stop event can be set to terminate the main loop
        self._terminate = threading.Event()
        self._terminate.clear()

    # ------------------------------------------------------------------------
    #
    def disable(self):
        """disable() stops the launcher, but leaves the state checking alive
        """
        logger.debug("launcher %s disabling" % (self.name))
        self._disabled.set()
        logger.debug("launcher %s disabled" % (self.name))


    # ------------------------------------------------------------------------
    #
    def stop(self):
        """stop() signals the process to finish up and terminate.
        """
        logger.debug("launcher %s stopping" % (self.name))
        self._terminate.set()
        self.join()
        logger.debug("launcher %s stopped" % (self.name))
      # logger.debug("Launcher thread (ID: %s[%s]) for PilotManager %s stopped." %
      #             (self.name, self.ident, self.pilot_manager_id))


    # ------------------------------------------------------------------------
    #
    def _get_pilot_logs (self, pilot_col, pilot_id) :

        out, err, log = ["", "", ""]
        # TODO: can this be linked to an #issue ?
        return out, err, log

        # attempt to get stdout/stderr/log.  We only expect those if pilot was
        # attempting launch at some point
        launched = False
        pilot    = pilot_col.find ({"_id": pilot_id})[0]

        for entry in pilot['statehistory'] :
            if entry['state'] == LAUNCHING :
                launched = True
                break

        if  launched :
            MAX_IO_LOGLENGTH = 10240    # 10k should be enough for anybody...

            try :
                f_out = saga.filesystem.File ("%s/%s" % (pilot['sandbox'], 'agent_0.out'))
                out   = f_out.read()[-MAX_IO_LOGLENGTH:]
                f_out.close ()
            except :
                pass

            try :
                f_err = saga.filesystem.File ("%s/%s" % (pilot['sandbox'], 'agent_0.err'))
                err   = f_err.read()[-MAX_IO_LOGLENGTH:]
                f_err.close ()
            except :
                pass

            try :
                f_log = saga.filesystem.File ("%s/%s" % (pilot['sandbox'], 'agent_0.log'))
                log   = f_log.read()[-MAX_IO_LOGLENGTH:]
                f_log.close ()
            except :
                pass

        return out, err, log


    # --------------------------------------------------------------------------
    #
    def check_pilot_states (self, pilot_col) :

        pending_pilots = pilot_col.find(
            {"pilotmanager": self.pilot_manager_id,
             "state"       : {"$in": [PENDING_ACTIVE, ACTIVE]},
             "health_check_enabled": True}
        )

        for pending_pilot in pending_pilots:

            pilot_failed = False
            pilot_done   = False
            reconnected  = False
            pilot_id     = pending_pilot["_id"]
            log_message  = ""
            saga_job_id  = pending_pilot["saga_job_id"]

            logger.info("Performing periodical health check for %s (SAGA job id %s)" % (str(pilot_id), saga_job_id))

            if  not pilot_id in self.missing_pilots :
                self.missing_pilots[pilot_id] = 0

            # Create a job service object:
            try:
                js_url = saga_job_id.split("]-[")[0][1:]

                if  js_url in self._shared_worker_data['job_services'] :
                    js = self._shared_worker_data['job_services'][js_url]
                else :
                    js = saga.job.Service(js_url, session=self._session)
                    self._shared_worker_data['job_services'][js_url] = js

                saga_job     = js.get_job(saga_job_id)
                reconnected  = True

                if  saga_job.state in [saga.job.FAILED, saga.job.CANCELED] :
                    pilot_failed = True
                    log_message  = "SAGA job state for ComputePilot %s is %s."\
                                 % (pilot_id, saga_job.state)

                if  saga_job.state in [saga.job.DONE] :
                    pilot_done = True
                    log_message  = "SAGA job state for ComputePilot %s is %s."\
                                 % (pilot_id, saga_job.state)

            except Exception as e:

                if  not reconnected :
                    logger.warning ('could not reconnect to pilot for state check (%s)' % e)
                    self.missing_pilots[pilot_id] += 1

                    if  self.missing_pilots[pilot_id] >= JOB_CHECK_MAX_MISSES :
                        logger.debug ('giving up after 10 attempts')
                        pilot_failed = True
                        log_message  = "Could not reconnect to pilot %s "\
                                       "multiple times - giving up" % pilot_id
                else :
                    logger.warning ('pilot state check failed: %s' % e)
                    pilot_failed = True
                    log_message  = "Couldn't determine job state for ComputePilot %s. " \
                                   "Assuming it has failed." % pilot_id


            if  pilot_failed :
                out, err, log = self._get_pilot_logs (pilot_col, pilot_id)
                ts = timestamp()
                pilot_col.update(
                    {"_id"  : pilot_id,
                     "state": {"$ne"     : DONE}},
                    {"$set" : {
                        "state"          : FAILED,
                        "stdout"         : out,
                        "stderr"         : err,
                        "logfile"        : log
                        },
                     "$push": {
                         "statehistory"  : {
                             "state"     : FAILED,
                             "timestamp" : ts
                             },
                         "log": {
                             "message"   : log_message,
                             "timestamp" : ts
                             }
                         }
                     }
                )
                logger.debug (log_message)
                logger.warn  ('pilot %s declared dead' % pilot_id)


            elif pilot_done :
                # FIXME: this should only be done if the state is not yet
                # done...
                out, err, log = self._get_pilot_logs (pilot_col, pilot_id)
                ts = timestamp()
                pilot_col.update(
                    {"_id"  : pilot_id,
                     "state": {"$ne"     : DONE}},
                    {"$set" : {
                        "state"          : DONE,
                        "stdout"         : out,
                        "stderr"         : err,
                        "logfile"        : log},
                     "$push": {
                         "statehistory"  : {
                             "state"     : DONE,
                             "timestamp" : ts
                             },
                         "log": {
                             "message"   : log_message,
                             "timestamp" : ts
                             }
                         }
                     }
                )
                logger.debug (log_message)
                logger.warn  ('pilot %s declared dead' % pilot_id)

            else :
                if self.missing_pilots[pilot_id] :
                    logger.info ('pilot %s *assumed* alive and well (%s)' \
                              % (pilot_id, self.missing_pilots[pilot_id]))
                else :
                    logger.info ('pilot %s seems alive and well' \
                              % (pilot_id))


    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the process when Process.start() is called.
        """

        global JOB_CHECK_INTERVAL

        # make sure to catch sys.exit (which raises SystemExit)
        try :
            # Get directory where this module lives
            mod_dir = os.path.dirname(os.path.realpath(__file__))

            # Try to connect to the database
            try:
                db = self._session.get_db()
                pilot_col = db["%s.p" % self._session.uid]
                logger.debug("Connected to MongoDB. Serving requests for PilotManager %s." % self.pilot_manager_id)

            except Exception as e :
                logger.exception ("Connection error: %s" % e)
                return

            last_job_check = time.time()

            while not self._terminate.is_set():

                # Periodically, we pull up all ComputePilots that are pending 
                # execution or were last seen executing and check if the corresponding  
                # SAGA job is still pending in the queue. If that is not the case, 
                # we assume that the job has failed for some reasons and update
                # the state of the ComputePilot accordingly.
                if  last_job_check + JOB_CHECK_INTERVAL < time.time() :
                    last_job_check = time.time()
                    self.check_pilot_states (pilot_col)

                if self._disabled.is_set():
                    # don't process any new pilot start requests.  
                    # NOTE: this is not clean, in principle there could be other
                    #       launchers alive which want to still start those 
                    #       pending pilots.  In practice we only ever use one
                    #       pmgr though, and its during its shutdown that we get
                    #       here...
                    ts = timestamp()
                    compute_pilot = pilot_col.find_and_modify(
                        query={"pilotmanager": self.pilot_manager_id,
                               "state" : PENDING_LAUNCH},
                        update={"$set" : {"state": CANCELED},
                                "$push": {"statehistory": {"state": CANCELED, "timestamp": ts}}}
                    )

                    # run state checks more frequently.
                    JOB_CHECK_INTERVAL = 3
                    time.sleep(1)
                    continue


                # See if we can find a ComputePilot that is waiting to be launched.
                # If we find one, we use SAGA to create a job service, a job
                # description and a job that is then send to the local or remote
                # queueing system. If this succedes, we set the ComputePilot's
                # state to pending, otherwise to failed.
                compute_pilot = None

                ts = timestamp()
                compute_pilot = pilot_col.find_and_modify(
                    query={"pilotmanager": self.pilot_manager_id,
                           "state" : PENDING_LAUNCH},
                    update={"$set" : {"state": LAUNCHING},
                            "$push": {"statehistory": {"state": LAUNCHING, "timestamp": ts}}}
                )

                if  not compute_pilot :
                    time.sleep(IDLE_TIMER)

                else:
                    try:
                        # ------------------------------------------------------
                        #
                        # LAUNCH THE PILOT AGENT VIA SAGA
                        #
                        logentries = []
                        pilot_id   = str(compute_pilot["_id"])

                        logger.info("Launching ComputePilot %s" % pilot_id)

                        # ------------------------------------------------------
                        # Database connection parameters
                        session_id    = self._session.uid
                        database_url  = self._session.dburl

                        # ------------------------------------------------------
                        # pilot description and resource configuration
                        number_cores    = compute_pilot['description']['cores']
                        runtime         = compute_pilot['description']['runtime']
                        queue           = compute_pilot['description']['queue']
                        project         = compute_pilot['description']['project']
                        cleanup         = compute_pilot['description']['cleanup']
                        resource_key    = compute_pilot['description']['resource']
                        schema          = compute_pilot['description']['access_schema']
                        memory          = compute_pilot['description']['memory']
                        candidate_hosts = compute_pilot['description']['candidate_hosts']
                        pilot_sandbox   = compute_pilot['sandbox']
                        global_sandbox  = compute_pilot['global_sandbox']

                        # we expand and exchange keys in the resource config,
                        # depending on the selected schema so better use a deep
                        # copy..
                        resource_cfg = self._session.get_resource_config(resource_key, schema)

                        # import pprint
                        # pprint.pprint (resource_cfg)

                        # ------------------------------------------------------
                        # get parameters from cfg, set defaults where needed
                        agent_launch_method     = resource_cfg.get ('agent_launch_method')
                        agent_dburl             = resource_cfg.get ('agent_mongodb_endpoint', database_url)
                        agent_spawner           = resource_cfg.get ('agent_spawner',       DEFAULT_AGENT_SPAWNER)
                        agent_type              = resource_cfg.get ('agent_type',          DEFAULT_AGENT_TYPE)
                        rc_agent_config         = resource_cfg.get ('agent_config',        DEFAULT_AGENT_CONFIG)
                        agent_scheduler         = resource_cfg.get ('agent_scheduler')
                        tunnel_bind_device      = resource_cfg.get ('tunnel_bind_device')
                        default_queue           = resource_cfg.get ('default_queue')
                        forward_tunnel_endpoint = resource_cfg.get ('forward_tunnel_endpoint')
                        js_endpoint             = resource_cfg.get ('job_manager_endpoint')
                        lrms                    = resource_cfg.get ('lrms')
                        mpi_launch_method       = resource_cfg.get ('mpi_launch_method')
                        pre_bootstrap_1         = resource_cfg.get ('pre_bootstrap_1')
                        pre_bootstrap_2         = resource_cfg.get ('pre_bootstrap_2')
                        python_interpreter      = resource_cfg.get ('python_interpreter')
                        spmd_variation          = resource_cfg.get ('spmd_variation')
                        task_launch_method      = resource_cfg.get ('task_launch_method')
                        rp_version              = resource_cfg.get ('rp_version',          DEFAULT_RP_VERSION)
                        virtenv_mode            = resource_cfg.get ('virtenv_mode',        DEFAULT_VIRTENV_MODE)
                        virtenv                 = resource_cfg.get ('virtenv',             DEFAULT_VIRTENV)
                        stage_cacerts           = resource_cfg.get ('stage_cacerts',       'False')
                        cores_per_node          = resource_cfg.get ('cores_per_node')
                        shared_filesystem       = resource_cfg.get ('shared_filesystem', True)
                        health_check            = resource_cfg.get ('health_check', True)
                        python_dist             = resource_cfg.get ('python_dist', DEFAULT_PYTHON_DIST)


                        # Agent configuration that is not part of the public API.
                        # The agent config can either be a config dict, or
                        # a string pointing to a configuration name.  If neither
                        # is given, check if 'RADICAL_PILOT_AGENT_CONFIG' is
                        # set.  The last fallback is 'agent_default'
                        agent_config = compute_pilot['description'].get('_config')
                        if not agent_config:
                            agent_config = os.environ.get('RADICAL_PILOT_AGENT_CONFIG')
                        if not agent_config:
                            agent_config = rc_agent_config

                        if isinstance(agent_config, dict):
                            # nothing to do
                            agent_cfg_dict = agent_config
                            pass

                        elif isinstance(agent_config, basestring):
                            try:
                                if os.path.exists(agent_config):
                                    # try to open as file name
                                    logger.info("Read agent config file: %s" % agent_config)
                                    agent_cfg_dict = ru.read_json(agent_config)
                                else:
                                    # otherwise interpret as a config name
                                    # FIXME: load in session just like resource
                                    #        configs, including user level overloads
                                    module_path = os.path.dirname(os.path.abspath(__file__))
                                    config_path = "%s/../configs/" % module_path
                                    agent_cfg_file = os.path.join(config_path, "agent_%s.json" % agent_config)
                                    logger.info("Read agent config file: %s" % agent_cfg_file)
                                    agent_cfg_dict = ru.read_json(agent_cfg_file)
                            except Exception as e:
                                logger.exception("Error reading agent config file: %s" % e)
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
                        virtenv = virtenv % {'pilot_sandbox' : saga.Url(pilot_sandbox).path,
                                             'global_sandbox': saga.Url(global_sandbox).path }

                        # Check for deprecated global_virtenv
                        global_virtenv = resource_cfg.get('global_virtenv')
                        if global_virtenv:
                            logger.warn ("'global_virtenv' keyword is deprecated -- use 'virtenv' and 'virtenv_mode'")
                            virtenv = global_virtenv
                            virtenv_mode = 'use'

                        # Create a host:port string for use by the bootstrap_1.
                        db_url = saga.Url(agent_dburl)
                        if db_url.port:
                            db_hostport = "%s:%d" % (db_url.host, db_url.port)
                        else:
                            db_hostport = "%s:%d" % (db_url.host, 27017) # mongodb default

                        # Open the remote sandbox
                        # TODO: make conditional on shared_fs?
                        sandbox_tgt = saga.filesystem.Directory(pilot_sandbox,
                                                                session=self._session,
                                                                flags=saga.filesystem.CREATE_PARENTS)

                        LOCAL_SCHEME = 'file'

                        # ------------------------------------------------------
                        # Copy the bootstrap shell script.
                        # This also creates the sandbox.
                        BOOTSTRAPPER_SCRIPT = "bootstrap_1.sh"
                        bootstrapper_path   = os.path.abspath("%s/../bootstrapper/%s" \
                                % (mod_dir, BOOTSTRAPPER_SCRIPT))

                        msg = "Using bootstrapper %s" % bootstrapper_path
                        logentries.append(Logentry(msg, logger=logger.info))

                        bs_script_url = saga.Url("%s://localhost%s" % (LOCAL_SCHEME, bootstrapper_path))

                        msg = "Copying bootstrapper '%s' to agent sandbox (%s)." \
                                % (bs_script_url, sandbox_tgt)
                        logentries.append(Logentry (msg, logger=logger.debug))

                        if shared_filesystem:
                            sandbox_tgt.copy(bs_script_url, BOOTSTRAPPER_SCRIPT)

                        # ------------------------------------------------------
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
                        # to mod_dir, or absolute).
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


                        # ------------------------------------------------------
                        # Copy the rp sdist if needed.  We actually also stage
                        # the sdists for radical.utils and radical.saga, so that
                        # we have the complete stack to install...
                        if stage_sdist:

                            for sdist_path in [ru.sdist_path, saga.sdist_path, rp_sdist_path]:

                                sdist_url = saga.Url("%s://localhost%s" % (LOCAL_SCHEME, sdist_path))
                                msg = "Copying sdist '%s' to sandbox (%s)." % (sdist_url, pilot_sandbox)
                                logentries.append(Logentry (msg, logger=logger.debug))
                                if shared_filesystem:
                                    sandbox_tgt.copy(sdist_url, os.path.basename(str(sdist_url)))


                        # ------------------------------------------------------
                        # Some machines cannot run pip due to outdated CA certs.
                        # For those, we also stage an updated certificate bundle
                        if stage_cacerts:
                            cc_path = os.path.abspath("%s/../bootstrapper/%s" \
                                    % (mod_dir, 'cacert.pem.gz'))

                            cc_url= saga.Url("%s://localhost/%s" % (LOCAL_SCHEME, cc_path))
                            msg = "Copying CA certificate bundle '%s' to sandbox (%s)." % (cc_url, pilot_sandbox)
                            logentries.append(Logentry (msg, logger=logger.debug))
                            if shared_filesystem:
                                sandbox_tgt.copy(cc_url, os.path.basename(str(cc_url)))


                        # ------------------------------------------------------
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

                        sdists = ':'.join([ru.sdist_name, saga.sdist_name, rp_sdist_name])

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
                        bootstrap_args += " -p '%s'" % pilot_id
                        bootstrap_args += " -r '%s'" % rp_version
                        bootstrap_args += " -s '%s'" % session_id
                        bootstrap_args += " -v '%s'" % virtenv
                        bootstrap_args += " -b '%s'" % python_dist

                        # set optional args
                        if agent_type:              bootstrap_args += " -a '%s'" % agent_type
                        if lrms == "CCM":           bootstrap_args += " -c"
                        if pre_bootstrap_1:         bootstrap_args += " -e '%s'" % "' -e '".join (pre_bootstrap_1)
                        if pre_bootstrap_2:         bootstrap_args += " -w '%s'" % "' -w '".join (pre_bootstrap_2)
                        if forward_tunnel_endpoint: bootstrap_args += " -f '%s'" % forward_tunnel_endpoint
                        if forward_tunnel_endpoint: bootstrap_args += " -h '%s'" % db_hostport
                        if python_interpreter:      bootstrap_args += " -i '%s'" % python_interpreter
                        if tunnel_bind_device:      bootstrap_args += " -t '%s'" % tunnel_bind_device
                        if cleanup:                 bootstrap_args += " -x '%s'" % cleanup

                        # set some agent configuration
                        agent_cfg_dict['cores']              = number_cores
                        agent_cfg_dict['debug']              = os.environ.get('RADICAL_PILOT_AGENT_VERBOSE', logger.getEffectiveLevel())
                        agent_cfg_dict['mongodb_url']        = str(agent_dburl)
                        agent_cfg_dict['lrms']               = lrms
                        agent_cfg_dict['spawner']            = agent_spawner
                        agent_cfg_dict['scheduler']          = agent_scheduler
                        agent_cfg_dict['runtime']            = runtime
                        agent_cfg_dict['pilot_id']           = pilot_id
                        agent_cfg_dict['session_id']         = session_id
                        agent_cfg_dict['agent_launch_method']= agent_launch_method
                        agent_cfg_dict['task_launch_method'] = task_launch_method
                        if mpi_launch_method:
                            agent_cfg_dict['mpi_launch_method']  = mpi_launch_method
                        if cores_per_node:
                            agent_cfg_dict['cores_per_node'] = cores_per_node

                        # ------------------------------------------------------
                        # Write agent config dict to a json file in pilot sandbox.

                        cfg_tmp_dir = tempfile.mkdtemp(prefix='rp_agent_cfg_dir')
                        agent_cfg_name = 'agent_0.cfg'
                        cfg_tmp_file = os.path.join(cfg_tmp_dir, agent_cfg_name)
                        cfg_tmp_handle = os.open(cfg_tmp_file, os.O_WRONLY|os.O_CREAT)

                        # Convert dict to json file
                        msg = "Writing agent configuration to file '%s'." % cfg_tmp_file
                        logentries.append(Logentry (msg, logger=logger.debug))
                        ru.write_json(agent_cfg_dict, cfg_tmp_file)

                        cf_url = saga.Url("%s://localhost%s" % (LOCAL_SCHEME, cfg_tmp_file))
                        msg = "Copying agent configuration file '%s' to sandbox (%s)." % (cf_url, pilot_sandbox)
                        logentries.append(Logentry (msg, logger=logger.debug))
                        if shared_filesystem:
                            sandbox_tgt.copy(cf_url, agent_cfg_name)

                        # Close agent config file
                        os.close(cfg_tmp_handle)

                        # ------------------------------------------------------
                        # Done with all transfers to pilot sandbox, close handle
                        sandbox_tgt.close()

                        # ------------------------------------------------------
                        # now that the scripts are in place and configured, 
                        # we can launch the agent
                        js_url = saga.Url(js_endpoint)
                        logger.debug ("saga.job.Service ('%s')" % js_url)
                        if  js_url in self._shared_worker_data['job_services'] :
                            js = self._shared_worker_data['job_services'][js_url]
                        else :
                            js = saga.job.Service(js_url, session=self._session)
                            self._shared_worker_data['job_services'][js_url] = js


                        # ------------------------------------------------------
                        # Create SAGA Job description and submit the pilot job

                        jd = saga.job.Description()

                        jd.executable            = "/bin/bash"
                        jd.arguments             = ["-l %s" % BOOTSTRAPPER_SCRIPT, bootstrap_args]
                        jd.working_directory     = saga.Url(pilot_sandbox).path
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
                                #'%s > %s' % (bootstrapper_path, os.path.basename(bootstrapper_path)),
                                '%s > %s' % (bootstrapper_path, os.path.join(jd.working_directory, 'input', os.path.basename(bootstrapper_path))),
                                '%s > %s' % (cfg_tmp_file, os.path.join(jd.working_directory, 'input', agent_cfg_name)),
                                #'%s < %s' % ('agent.log', os.path.join(jd.working_directory, 'agent.log')),
                                #'%s < %s' % (os.path.join(jd.working_directory, 'agent.log'), 'agent.log'),
                                #'%s < %s' % ('agent.log', 'agent.log'),
                                #'%s < %s' % (os.path.join(jd.working_directory, 'STDOUT'), 'unit.000000/STDOUT'),
                                #'%s < %s' % (os.path.join(jd.working_directory, 'unit.000000/STDERR'), 'STDERR')
                                #'%s < %s' % ('unit.000000/STDERR', 'unit.000000/STDERR')

                                # TODO: This needs to go into a per pilot directory on the submit node
                                '%s < %s' % ('pilot.0000.log.tgz', 'pilot.0000.log.tgz')
                            ]

                            if stage_sdist:
                                jd.file_transfer.extend([
                                    #'%s > %s' % (rp_sdist_path, os.path.basename(rp_sdist_path)),
                                    '%s > %s' % (rp_sdist_path, os.path.join(jd.working_directory, 'input', os.path.basename(rp_sdist_path))),
                                    #'%s > %s' % (saga.sdist_path, os.path.basename(saga.sdist_path)),
                                    '%s > %s' % (saga.sdist_path, os.path.join(jd.working_directory, 'input', os.path.basename(saga.sdist_path))),
                                    #'%s > %s' % (ru.sdist_path, os.path.basename(ru.sdist_path)),
                                    '%s > %s' % (ru.sdist_path, os.path.join(jd.working_directory, 'input', os.path.basename(ru.sdist_path)))
                                ])

                            if stage_cacerts:
                                jd.file_transfer.append('%s > %s' % (cc_path, os.path.join(jd.working_directory, 'input', os.path.basename(cc_path))))

                            if 'RADICAL_PILOT_PROFILE' in os.environ :
                                # TODO: This needs to go into a per pilot directory on the submit node
                                jd.file_transfer.append('%s < %s' % ('pilot.0000.prof.tgz', 'pilot.0000.prof.tgz'))

                        # Set the SPMD variation only if required
                        if spmd_variation:
                            jd.spmd_variation = spmd_variation

                        if 'RADICAL_PILOT_PROFILE' in os.environ :
                            jd.environment['RADICAL_PILOT_PROFILE'] = 'TRUE'

                        logger.debug("Bootstrap command line: %s %s" % (jd.executable, jd.arguments))

                        msg = "Submitting SAGA job with description: %s" % str(jd.as_dict())
                        logentries.append(Logentry (msg, logger=logger.debug))

                        pilotjob = js.create_job(jd)
                        pilotjob.run()

                        # Clean up agent config file after submission
                        os.unlink(cfg_tmp_file)

                        # do a quick error check
                        if pilotjob.state == saga.FAILED:
                            raise RuntimeError ("SAGA Job state is FAILED.")

                        saga_job_id = pilotjob.id
                        self._shared_worker_data['job_ids'][pilot_id] = [saga_job_id, js_url]

                        msg = "SAGA job submitted with job id %s" % str(saga_job_id)
                        logentries.append(Logentry (msg, logger=logger.debug))

                        #
                        # ------------------------------------------------------

                        log_dicts = list()
                        for le in logentries :
                            log_dicts.append (le.as_dict())

                        # Update the Pilot's state to 'PENDING_ACTIVE' if SAGA job submission was successful.
                        ts = timestamp()
                        ret = pilot_col.update(
                            {"_id"  : pilot_id,
                             "state": LAUNCHING},
                            {"$set" : {"state": PENDING_ACTIVE,
                                       "saga_job_id": saga_job_id,
                                       "health_check_enabled": health_check,
                                       "agent_config": agent_cfg_dict},
                             "$push": {"statehistory": {"state": PENDING_ACTIVE, "timestamp": ts}},
                             "$pushAll": {"log": log_dicts}
                            }
                        )

                        if  ret['n'] == 0 :
                            # could not update, probably because the agent is
                            # running already.  Just update state history and
                            # jobid then
                            # FIXME: make sure of the agent state!
                            ret = pilot_col.update(
                                {"_id"  : pilot_id},
                                {"$set" : {"saga_job_id": saga_job_id,
                                           "health_check_enabled": health_check},
                                 "$push": {"statehistory": {"state": PENDING_ACTIVE, "timestamp": ts}},
                                 "$pushAll": {"log": log_dicts}}
                            )


                    except Exception as e:
                        # Update the Pilot's state 'FAILED'.
                        out, err, log = self._get_pilot_logs (pilot_col, pilot_id)
                        ts = timestamp()

                        # FIXME: we seem to be unable to bson/json handle saga
                        # log messages containing an '#'.  This shows up here.
                        # Until we find a clean workaround, make log shorter and
                        # rely on saga logging to reveal the problem.
                        msg = "Pilot launching failed! (%s)" % e
                        logentries.append (Logentry (msg))

                        log_dicts    = list()
                        log_messages = list()
                        for le in logentries :
                            log_dicts.append (le.as_dict())
                            log_messages.append (str(le.message))

                        pilot_col.update(
                            {"_id"  : pilot_id,
                             "state": {"$ne" : FAILED}},
                            {"$set" : {
                                "state"   : FAILED,
                                "stdout"  : out,
                                "stderr"  : err,
                                "logfile" : log},
                             "$push": {"statehistory": {"state"    : FAILED,
                                                        "timestamp": ts}},
                             "$pushAll": {"log": log_dicts}}
                        )
                        logger.exception ('\n'.join (log_messages))

        except SystemExit as e :
            logger.exception("pilot launcher thread caught system exit -- forcing application shutdown")
            import thread
            thread.interrupt_main ()
