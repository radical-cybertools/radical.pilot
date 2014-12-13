"""
.. module:: radical.pilot.controller.pilot_launcher_worker
.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import datetime
import traceback
import threading
import radical.utils as ru

from bson.objectid import ObjectId

from radical.pilot.states import * 

from radical.pilot.utils.version import version as VERSION
from radical.pilot.utils.logger import logger
from radical.pilot.context import Context

# BULK_LIMIT defines the max. number of transfer requests to pull from DB.
BULK_LIMIT=1

# The interval at which we check the saga jobs.
JOB_CHECK_INTERVAL   = 60 # seconds
JOB_CHECK_MAX_MISSES =  3 # how often to miss a job be declaring it dead

# ----------------------------------------------------------------------------
#
class PilotLauncherWorker(threading.Thread):
    """PilotLauncherWorker handles bootstrapping and launching of
       the pilot agents.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, session, db_connection_info, pilot_manager_id, 
                 shared_worker_data, number=None):
        """Creates a new pilot launcher background process.
        """
        self._session = session

        # threading stuff
        threading.Thread.__init__(self)

        self.db_connection_info = db_connection_info
        self.pilot_manager_id   = pilot_manager_id
        self.name               = "PilotLauncherWorker-%s" % str(number)
        self.missing_pilots     = dict()
        self._shared_worker_data = shared_worker_data

        # Stop event can be set to terminate the main loop
        self._stop = threading.Event()
        self._stop.clear()

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """stop() signals the process to finish up and terminate.
        """
        logger.error("launcher %s stopping" % (self.name))
        self._stop.set()
        self.join()
        logger.error("launcher %s stopped" % (self.name))
      # logger.debug("Launcher thread (ID: %s[%s]) for PilotManager %s stopped." %
      #             (self.name, self.ident, self.pilot_manager_id))


    # ------------------------------------------------------------------------
    #
    def _get_pilot_logs (self, pilot_col, pilot_id) :

        out, err, log = ["", "", ""]
        return out, err, log

        # attempt to get stdout/stderr/log.  We only expect those if pilot was
        # attemptint launch at some point
        launched = False
        pilot    = pilot_col.find ({"_id": pilot_id})[0]

        for entry in pilot['statehistory'] :
            if entry['state'] == LAUNCHING :
                launched = True
                break

        if  launched :
            MAX_IO_LOGLENGTH = 10240    # 10k should be enough for anybody...

            try :
                f_out = saga.filesystem.File ("%s/%s" % (pilot['sandbox'], 'AGENT.STDOUT'))
                out   = f_out.read()[-MAX_IO_LOGLENGTH:]
                f_out.close ()
            except :
                pass

            try :
                f_err = saga.filesystem.File ("%s/%s" % (pilot['sandbox'], 'AGENT.STDERR'))
                err   = f_err.read()[-MAX_IO_LOGLENGTH:]
                f_err.close ()
            except :
                pass

            try :
                f_log = saga.filesystem.File ("%s/%s" % (pilot['sandbox'], 'AGENT.LOG'))
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
             "state"       : {"$in": [PENDING_ACTIVE, ACTIVE]}}
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
                        logger.error ('giving up after 10 attempts')
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
                ts = datetime.datetime.utcnow()
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
                             "logentry"  : log_message, 
                             "timestamp" : ts
                             }
                         }
                     }
                )
                logger.error (log_message)
                logger.error ('pilot %s declared dead' % pilot_id)


            elif pilot_done :
                # FIXME: this should only be done if the state is not yet
                # done...
                out, err, log = self._get_pilot_logs (pilot_col, pilot_id)
                ts = datetime.datetime.utcnow()
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
                             "logentry"  : log_message, 
                             "timestamp" : ts
                             }
                         }
                     }
                )
                logger.error (log_message)
                logger.error ('pilot %s declared dead' % pilot_id)

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

        # make sure to catch sys.exit (which raises SystemExit)
        try :

            # Try to connect to the database 
            try:
                connection = self.db_connection_info.get_db_handle()
                db = connection[self.db_connection_info.dbname]
                pilot_col = db["%s.p" % self.db_connection_info.session_id]
                logger.debug("Connected to MongoDB. Serving requests for PilotManager %s." % self.pilot_manager_id)

            except Exception, ex:
                tb = traceback.format_exc()
                logger.error("Connection error: %s. %s" % (str(ex), tb))
                return

            last_job_check = time.time()

            while not self._stop.is_set():

                # Periodically, we pull up all ComputePilots that are pending 
                # execution or were last seen executing and check if the corresponding  
                # SAGA job is still pending in the queue. If that is not the case, 
                # we assume that the job has failed for some reasons and update
                # the state of the ComputePilot accordingly.
                if  last_job_check + JOB_CHECK_INTERVAL < time.time() :
                    last_job_check = time.time()
                    self.check_pilot_states (pilot_col)


                # See if we can find a ComputePilot that is waiting to be launched.
                # If we find one, we use SAGA to create a job service, a job 
                # description and a job that is then send to the local or remote
                # queueing system. If this succedes, we set the ComputePilot's 
                # state to pending, otherwise to failed.
                compute_pilot = None

                ts = datetime.datetime.utcnow()
                compute_pilot = pilot_col.find_and_modify(
                    query={"pilotmanager": self.pilot_manager_id,
                           "state" : PENDING_LAUNCH},
                    update={"$set" : {"state": LAUNCHING},
                            "$push": {"statehistory": {"state": LAUNCHING, "timestamp": ts}}},
                    limit=BULK_LIMIT
                )

                if compute_pilot is None:
                    # Sleep a bit if no new pilots are available.
                    time.sleep(0.1)
                else:
                    try:
                        ######################################################################
                        ##
                        ## LAUNCH THE PILOT AGENT VIA SAGA
                        log_messages = []

                        pilot_id = str(compute_pilot["_id"])
                        logger.info("Launching ComputePilot %s" % compute_pilot)

                        number_cores = compute_pilot['description']['cores']
                        runtime      = compute_pilot['description']['runtime']
                        queue        = compute_pilot['description']['queue']
                        project      = compute_pilot['description']['project']
                        cleanup      = compute_pilot['description']['cleanup']
                        resource_key = compute_pilot['description']['resource']
                        schema       = compute_pilot['description']['access_schema']

                        sandbox      = compute_pilot['sandbox']

                        # check if the user specified a sandbox:
                        if  'sandbox' in compute_pilot['description'] and \
                            compute_pilot['description']['sandbox']   and \
                            compute_pilot['description']['sandbox'] == sandbox :
                            user_sandbox = True
                        else :
                            user_sandbox = False


                        resource_cfg = self._session.get_resource_config(resource_key)
                        agent_worker = resource_cfg.get ('pilot_agent_worker', None)

                        # we expand and exchange keys in the resource config,
                        # depending on the selected schema so better use a deep
                        # copy..
                        import copy
                        resource_cfg = copy.deepcopy (resource_cfg)

                        if  not schema :
                            if 'schemas' in resource_cfg :
                                schema = resource_cfg['schemas'][0]

                        if  not schema in resource_cfg :
                            logger.warning ("schema %s unknown for resource %s -- continue with defaults" \
                                         % (schema, resource_key))

                        else :
                            for key in resource_cfg[schema] :
                                # merge schema specific resource keys into the
                                # resource config
                                resource_cfg[key] = resource_cfg[schema][key]

                        ########################################################
                        # Database connection parameters
                        session_uid  = self.db_connection_info.session_id
                        database_url = self.db_connection_info.dburl

                        surl = saga.Url (database_url)

                        # Set default port if not specified
                        # (explicit is better than implicit!)
                        if not surl.port:
                            surl.port = 27017

                        # Set default host to localhost if not specified
                        if not surl.host:
                            surl.host = 'localhost'

                        database_name = self.db_connection_info.dbname
                        # Set default database name if not specified
                        if not database_name:
                            database_name = 'radicalpilot'

                        database_auth     = self.db_connection_info.dbauth
                        database_hostport = "%s:%d" % (surl.host, surl.port)


                        ########################################################
                        # Get directory where pilot_launcher_worker.py lives
                        plw_dir = os.path.dirname(os.path.realpath(__file__))

                        ########################################################
                        # take 'pilot_agent' as defined in the resource configuration
                        # by default, but override it if set in the Pilot description. 
                        pilot_agent = resource_cfg['pilot_agent']
                        if compute_pilot['description']['pilot_agent_priv'] is not None:
                            pilot_agent = compute_pilot['description']['pilot_agent_priv']
                        agent_path = os.path.abspath("%s/../agent/%s" % (plw_dir, pilot_agent))

                        log_msg = "Using pilot agent %s" % agent_path
                        log_messages.append({
                            "logentry": log_msg, 
                            "timestamp": datetime.datetime.utcnow()})
                        logger.info(log_msg)

                        ########################################################
                        # we use always "default_bootstrapper.sh" unless a resource 
                        # configuration explicitly defines another bootstrapper. 
                        if 'bootstrapper' in resource_cfg and resource_cfg['bootstrapper'] is not None:
                            bootstrapper = resource_cfg['bootstrapper']
                        else:
                            bootstrapper = 'default_bootstrapper.sh'
                        bootstrapper_path = os.path.abspath("%s/../bootstrapper/%s" % (plw_dir, bootstrapper))
                        
                        log_msg = "Using bootstrapper %s" % bootstrapper_path
                        log_messages.append({
                            "logentry": log_msg, 
                            "timestamp": datetime.datetime.utcnow()})
                        logger.info(log_msg)

                        ########################################################
                        # Create SAGA Job description and submit the pilot job #
                        ########################################################

                        # log_msg = "Creating agent sandbox '%s'." % str(sandbox)
                        # log_messages.append(log_msg)
                        # logger.debug(log_msg)
                        #
                        # logger.debug ("saga.fs.Directory ('%s')" % saga.Url(sandbox))
                        # agent_dir = saga.filesystem.Directory(
                        #     saga.Url(sandbox),
                        #     saga.filesystem.CREATE_PARENTS, session=self._session)
                        # agent_dir.close()

                        ########################################################
                        # Copy the bootstrap shell script.  This also creates
                        # the sandbox
                        bs_script_url = saga.Url("file://localhost/%s" % bootstrapper_path)
                        bs_script_tgt = saga.Url("%s/%s"               % (sandbox, bootstrapper))
                        log_msg = "Copying bootstrapper '%s' to agent sandbox (%s)." % (bs_script_url, bs_script_tgt)
                        log_messages.append({
                            "logentry": log_msg, 
                            "timestamp": datetime.datetime.utcnow()})
                        logger.debug(log_msg)

                        bs_script = saga.filesystem.File(bs_script_url, session=self._session)
                        bs_script.copy(bs_script_tgt, flags=saga.filesystem.CREATE_PARENTS)
                        bs_script.close()

                        ########################################################
                        # Copy the agent script
                        agent_script_url = saga.Url("file://localhost/%s" % agent_path)
                        log_msg = "Copying agent '%s' to agent sandbox (%s)." % (agent_script_url, sandbox)
                        log_messages.append({
                            "logentry": log_msg, 
                            "timestamp": datetime.datetime.utcnow()})
                        logger.debug(log_msg)

                        agent_script = saga.filesystem.File(agent_script_url)
                        agent_script.copy("%s/radical-pilot-agent.py" % str(sandbox))
                        agent_script.close()

                        # copying agent-worker.py script to sandbox
                        #########################################################

                        if agent_worker is not None:
                            logger.warning("Using custom agent worker script: %s" % agent_worker)
                            worker_path = os.path.abspath("%s/../agent/%s" % (plw_dir, agent_worker))

                            worker_script_url = saga.Url("file://localhost/%s" % worker_path)

                            log_msg = "Copying '%s' to agent sandbox (%s)." % (worker_script_url, sandbox)
                            log_messages.append({
                                "logentry": log_msg, 
                                "timestamp": datetime.datetime.utcnow()})
                            logger.debug(log_msg)

                            worker_script = saga.filesystem.File(worker_script_url)
                            worker_script.copy("%s/agent-worker.py" % str(sandbox))
                            worker_script.close()

                        #########################################################
                        # now that the script is in place and we know where it is,
                        # we can launch the agent
                        js_url = saga.Url(resource_cfg['job_manager_endpoint'])
                        logger.debug ("saga.job.Service ('%s')" % js_url)
                        if  js_url in self._shared_worker_data['job_services'] :
                            js = self._shared_worker_data['job_services'][js_url]
                        else :
                            js = saga.job.Service(js_url, session=self._session)
                            self._shared_worker_data['job_services'][js_url] = js

                        jd = saga.job.Description()
                        jd.working_directory = saga.Url(sandbox).path

                        bootstrap_args = "-n %s -s %s -p %s -t %s -c %s -v %s" %\
                            (database_name, session_uid, str(pilot_id),
                             runtime, number_cores, VERSION)

                        if  user_sandbox :
                            bootstrap_args += " -u"

                        if 'agent_mongodb_endpoint' in resource_cfg and resource_cfg['agent_mongodb_endpoint'] is not None:
                            agent_db_url = ru.Url(resource_cfg['agent_mongodb_endpoint'])
                            bootstrap_args += " -m %s:%d " % (agent_db_url.host, agent_db_url.port)
                        else:
                            bootstrap_args += " -m %s " % database_hostport
 
                        bootstrap_args += " -a %s " % database_auth

                        if 'python_interpreter' in resource_cfg and resource_cfg['python_interpreter'] is not None:
                            bootstrap_args += " -i %s " % resource_cfg['python_interpreter']
                        if 'pre_bootstrap' in resource_cfg and resource_cfg['pre_bootstrap'] is not None:
                            for command in resource_cfg['pre_bootstrap']:
                                bootstrap_args += " -e '%s' " % command
                        if 'global_virtenv' in resource_cfg and resource_cfg['global_virtenv'] is not None:
                            bootstrap_args += " -g %s " % resource_cfg['global_virtenv']
                        if 'lrms' in resource_cfg and resource_cfg['lrms'] is not None:
                            bootstrap_args += " -l %s " % resource_cfg['lrms']
                        else:
                            raise Exception("LRMS not specified.")
                        if 'task_launch_method' in resource_cfg and resource_cfg['task_launch_method'] is not None:
                            bootstrap_args += " -j %s " % resource_cfg['task_launch_method']
                        else:
                            raise Exception("Task launch method not set.")
                        if 'mpi_launch_method' in resource_cfg and resource_cfg['mpi_launch_method'] is not None:
                            bootstrap_args += " -k %s " % resource_cfg['mpi_launch_method']
                        else:
                            raise Exception("MPI launch method not set.")
                        if 'forward_tunnel_endpoint' in resource_cfg and resource_cfg['forward_tunnel_endpoint'] is not None:
                            bootstrap_args += " -f %s " % resource_cfg['forward_tunnel_endpoint']

                        if cleanup is True: 
                            # cleanup flags:
                            #   l : pilot log files
                            #   u : unit work dirs
                            #   v : virtualenv
                            #   e : everything (== pilot sandbox)
                            # FIXME: get cleanup flags from somewhere
                            logger.info ('request cleanup for pilot %s' % pilot_id)
                            bootstrap_args += " -x %s" % 'luve' # the cleanup flag

                        if 'RADICAL_PILOT_AGENT_VERBOSE' in os.environ :
                            debug_level = {
                                    'CRITICAL' : 1,
                                    'ERROR'    : 2,
                                    'WARNING'  : 3,
                                    'WARN'     : 3,
                                    'INFO'     : 4,
                                    'DEBUG'    : 5}.get (os.environ['RADICAL_PILOT_AGENT_VERBOSE'], 
                                                     int(os.environ['RADICAL_PILOT_AGENT_VERBOSE']))
                            bootstrap_args += " -d %s" % debug_level
                            bootstrap_args += " -b"  # also keep slot statuses around
                        else :
                            # fall back to local log level, if such one is set
                            if  logger.level :
                                bootstrap_args += " -d %s" % logger.level
                                bootstrap_args += " -b"  # also keep slot statuses around

                        if  'RADICAL_PILOT_BENCHMARK' in os.environ :
                            if  not " -b" in bootstrap_args :
                                bootstrap_args += " -b"

                        jd.executable = "/bin/bash"
                        jd.arguments = ["-l", bootstrapper, bootstrap_args]

                        logger.debug("Bootstrap command line: %s %s" % (jd.executable, jd.arguments))

                        # fork:// and ssh:// don't support 'queue' and 'project'
                        if (js_url.schema != "fork") and (js_url.schema != "ssh"):

                            # process the 'queue' attribute
                            if queue is not None:
                                jd.queue = queue
                            elif 'default_queue' in resource_cfg:
                                jd.queue = resource_cfg['default_queue']

                            # process the project / allocation 
                            if project is not None:
                                jd.project = project

                        # set the SPMD variation if required
                        if 'spmd_variation' in resource_cfg and resource_cfg['spmd_variation'] is not None:
                            jd.spmd_variation = resource_cfg['spmd_variation']

                        jd.output = "AGENT.STDOUT"
                        jd.error  = "AGENT.STDERR"
                        jd.total_cpu_count = number_cores
                        jd.wall_time_limit = runtime
                        if compute_pilot['description']['memory'] is not None:
                            jd.total_physical_memory = compute_pilot['description']['memory']

                        log_msg = "Submitting SAGA job with description: %s" % str(jd.as_dict())
                        log_messages.append({
                            "logentry": log_msg, 
                            "timestamp": datetime.datetime.utcnow()})
                        logger.debug(log_msg)

                        pilotjob = js.create_job(jd)
                        pilotjob.run()

                        # do a quick error check
                        if pilotjob.state == saga.FAILED:
                            raise Exception("SAGA Job state was FAILED.")

                        saga_job_id = pilotjob.id
                        log_msg = "SAGA job submitted with job id %s" % str(saga_job_id)

                        self._shared_worker_data['job_ids'][pilot_id] = [saga_job_id, js_url]

                        log_messages.append({
                            "logentry": log_msg, 
                            "timestamp": datetime.datetime.utcnow()})
                        logger.debug(log_msg)

                        ##
                        ##
                        ######################################################################

                        # Update the Pilot's state to 'PENDING_ACTIVE' if SAGA job submission was successful.
                        ts = datetime.datetime.utcnow()
                        ret = pilot_col.update(
                            {"_id"  : ObjectId(pilot_id),
                             "state": 'Launching'},
                            {"$set" : {"state": PENDING_ACTIVE,
                                      "saga_job_id": saga_job_id},
                             "$push": {"statehistory": {"state": PENDING_ACTIVE, "timestamp": ts}},
                             "$pushAll": {"log": log_messages}
                            }
                        )

                        if  ret['n'] == 0 :
                            # could not update, probably because the agent is
                            # running already.  Just update state history and
                            # jobid then
                            # FIXME: make sure of the agent state!
                            ret = pilot_col.update(
                                {"_id"  : ObjectId(pilot_id)},
                                {"$set" : {"saga_job_id": saga_job_id},
                                 "$push": {"statehistory": {"state": PENDING_ACTIVE, "timestamp": ts}},
                                 "$pushAll": {"log": log_messages}}
                            )

                    except Exception, ex:
                        # Update the Pilot's state 'FAILED'.
                        out, err, log = self._get_pilot_logs (pilot_col, pilot_id)
                        ts = datetime.datetime.utcnow()

                        # FIXME: we seem to be unable to bson/json handle saga
                        # log messages containing an '#'.  This shows up here.
                        # Until we find a clean workaround, make log shorter and
                        # rely on saga logging to reveal the problem.
                      # log_msg = "Pilot launching failed: %s\n%s" % (str(ex), traceback.format_exc())
                        log_msg = "Pilot launching failed!"
                        log_messages.append({
                            "logentry": log_msg, 
                            "timestamp": ts})

                        pilot_col.update(
                            {"_id"  : ObjectId(pilot_id),
                             "state": {"$ne" : FAILED}},
                            {"$set" : {
                                "state"   : FAILED,
                                "stdout"  : out,
                                "stderr"  : err,
                                "logfile" : log},
                             "$push": {"statehistory": {"state": FAILED, "timestamp": ts}},
                             "$pushAll": {"log": log_messages}}
                        )
                        logger.exception (log_messages)

        except SystemExit as e :
            logger.exception("pilot launcher thread caught system exit -- forcing application shutdown")
            import thread
            thread.interrupt_main ()
            

