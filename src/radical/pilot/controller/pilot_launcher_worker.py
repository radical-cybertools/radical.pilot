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
import multiprocessing

from radical.utils import which
from bson.objectid import ObjectId

from radical.pilot.states import * 

from radical.pilot.utils.version import version as VERSION
from radical.pilot.utils.logger import logger
from radical.pilot.credentials import SSHCredential

# BULK_LIMIT defines the max. number of transfer requests to pull from DB.
BULK_LIMIT=1

# The interval at which we check 
# the saga jobs.
JOB_CHECK_INTERVAL=10 # seconds

# ----------------------------------------------------------------------------
#
class PilotLauncherWorker(multiprocessing.Process):
    """PilotLauncherWorker handles bootstrapping and laucnhing of 
       the pilot agents.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, db_connection_info, pilot_manager_id, 
        resource_configurations, number=None):
        """Creates a new pilot launcher background process.
        """
        # Multiprocessing stuff
        multiprocessing.Process.__init__(self)
        self.daemon = True

        self.db_connection_info = db_connection_info
        self.pilot_manager_id = pilot_manager_id

        self.resource_configurations = resource_configurations

        self.name = "PilotLauncherWorker-%s" % str(number)

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the process when Process.start() is called.
        """

        # saga_session holds the SSH context infos.
        saga_session = saga.Session()

        # Try to connect to the database and create a tailable cursor.
        try:
            connection = self.db_connection_info.get_db_handle()
            db = connection[self.db_connection_info.dbname]
            pilot_col = db["%s.p" % self.db_connection_info.session_id]
            logger.debug("Connected to MongoDB. Serving requests for PilotManager %s." % self.pilot_manager_id)

            session_col = db["%s" % self.db_connection_info.session_id]
            session = session_col.find(
                {"_id": ObjectId(self.db_connection_info.session_id)},
                {"credentials": 1}
            )

            for cred_dict in session[0]["credentials"]:
                cred = SSHCredential.from_dict(cred_dict)
                saga_session.add_context(cred._context)
                logger.debug("Found SSH context info: %s." % cred._context)

        except Exception, ex:
            tb = traceback.format_exc()
            logger.error("Connection error: %s. %s" % (str(ex), tb))
            return

        last_job_check = time.time()

        while True:
            # Periodically, we pull up all ComputePilots that are pending 
            # execution and check if the corresponding  SAGA
            # job is still pending in the queue. If that is not the case, 
            # we assume that the job has failed for some reasons and update
            # the state of the ComputePilot accordingly.
            if last_job_check + JOB_CHECK_INTERVAL < time.time():
                pending_pilots = pilot_col.find(
                    {"pilotmanager": self.pilot_manager_id,
                     "state"       : PENDING_EXECUTION}
                )

                for pending_pilot in pending_pilots:
                    pilot_id    = pending_pilot["_id"]
                    saga_job_id = pending_pilot["saga_job_id"]
                    logger.info("Performing periodical health check for %s (SAGA job id %s)" % (str(pilot_id), saga_job_id))

                    log_message = None

                    # Create a job service object:
                    try: 
                        js_url = saga_job_id.split("]-[")[0][1:]
                        js = saga.job.Service(js_url, session=saga_session)
                        saga_job = js.get_job(saga_job_id)
                        if saga.job.state == saga.job.FAILED:
                            log_message = "SAGA job state for ComputePilot %s is FAILED." % pilot_id

                            ts = datetime.datetime.utcnow()
                            pilot_col.update(
                                {"_id": pilot_id},
                                {"$set": {"state": FAILED},
                                 "$push": {"statehistory": {"state": FAILED, "timestamp": ts}},
                                 "$push": {"log": log_message}}
                            )
                            logger.error(log_message)
                        js.close()

                    except Exception, ex:
                        log_message = "Couldn't determine SAGA job state for ComputePilot %s. Assuming it failed to launch." % pilot_id

                        ts = datetime.datetime.utcnow()
                        pilot_col.update(
                            {"_id": pilot_id},
                            {"$set": {"state": FAILED},
                             "$push": {"statehistory": {"state": FAILED, "timestamp": ts}},
                             "$push": {"log": log_message}}
                        )
                        logger.error(log_message)

                # Update timer
                last_job_check = time.time()

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
                # Sleep a bit if no new units are available.
                time.sleep(1)
            else:
                try:
                    ######################################################################
                    ##
                    ## LAUNCH THE PILOT AGENT VIA SAGA
                    log_messages = []

                    compute_pilot_id = str(compute_pilot["_id"])
                    logger.info("Launching ComputePilot %s" % compute_pilot)

                    number_cores = compute_pilot['description']['cores']
                    runtime      = compute_pilot['description']['runtime']
                    queue        = compute_pilot['description']['queue']
                    project      = compute_pilot['description']['project']
                    cleanup      = compute_pilot['description']['cleanup']
                    #pilot_agent  = compute_pilot['description']['pilot_agent_priv']
                    #agent_worker = compute_pilot['description']['agent_worker']
                    sandbox      = compute_pilot['sandbox']

                    use_local_endpoints = False
                    resource_key = compute_pilot['description']['resource']
                    s = compute_pilot['description']['resource'].split(":")
                    if len(s) == 2:
                        if s[1].lower() == "local":
                            use_local_endpoints = True
                            resource_key = s[0]
                        else:
                            error_msg = "Unknown resource qualifier '%s' in %s." % (s[1], compute_pilot['description']['resource'])
                            raise Exception(error_msg)

                    resource_cfg = self.resource_configurations[resource_key]

                    if 'pilot_agent_worker' in resource_cfg and resource_cfg['pilot_agent_worker'] is not None:
                        agent_worker = resource_cfg['pilot_agent_worker']
                    else:
                        agent_worker = None

                    ########################################################
                    # database connection parameters
                    database_host = self.db_connection_info.url.split("://")[1] 
                    database_name = self.db_connection_info.dbname
                    session_uid   = self.db_connection_info.session_id

                    cwd = os.path.dirname(os.path.realpath(__file__))

                    ########################################################
                    # take 'pilot_agent' as defined in the reosurce configuration
                    # by default, but override it if set in the Pilot description. 
                    pilot_agent = resource_cfg['pilot_agent']
                    if compute_pilot['description']['pilot_agent_priv'] is not None:
                        pilot_agent = compute_pilot['description']['pilot_agent_priv']
                    agent_path = os.path.abspath("%s/../agent/%s" % (cwd, pilot_agent))

                    log_msg = "Using pilot agent %s" % agent_path
                    log_messages.append(log_msg)
                    logger.info(log_msg)

                    ########################################################
                    # we use always "default_bootstrapper.sh" unless a resource 
                    # configuration explicitly defines another bootstrapper. 
                    if 'bootstrapper' in resource_cfg and resource_cfg['bootstrapper'] is not None:
                        bootstrapper = resource_cfg['bootstrapper']
                    else:
                        bootstrapper = 'default_bootstrapper.sh'
                    bootstrapper_path = os.path.abspath("%s/../bootstrapper/%s" % (cwd, bootstrapper))
                    
                    log_msg = "Using bootstrapper %s" % bootstrapper_path
                    log_messages.append(log_msg)
                    logger.info(log_msg)

                    ########################################################
                    # Create SAGA Job description and submit the pilot job #
                    ########################################################

                    log_msg = "Creating agent sandbox '%s'." % str(sandbox)
                    log_messages.append(log_msg)
                    logger.debug(log_msg)

                    agent_dir = saga.filesystem.Directory(
                        saga.Url(sandbox),
                        saga.filesystem.CREATE_PARENTS, session=saga_session)
                    agent_dir.close()

                    ########################################################
                    # Copy the bootstrap shell script
                    bs_script_url = saga.Url("file://localhost/%s" % bootstrapper_path)
                    log_msg = "Copying bootstrapper '%s' to agent sandbox (%s)." % (bs_script_url, sandbox)
                    log_messages.append(log_msg)
                    logger.debug(log_msg)

                    bs_script = saga.filesystem.File(bs_script_url)
                    bs_script.copy(saga.Url(sandbox))
                    bs_script.close()

                    ########################################################
                    # Copy the agent script
                    agent_script_url = saga.Url("file://localhost/%s" % agent_path)
                    log_msg = "Copying agent '%s' to agent sandbox (%s)." % (agent_script_url, sandbox)
                    log_messages.append(log_msg)
                    logger.debug(log_msg)

                    agent_script = saga.filesystem.File(agent_script_url)
                    agent_script.copy("%s/radical-pilot-agent.py" % str(sandbox))
                    agent_script.close()

                    # copying agent-worker.py script to sandbox
                    #########################################################
                    cwd = os.path.dirname(os.path.abspath(__file__))

                    if agent_worker is not None:
                        logger.warning("Using custom agent worker script: %s" % agent_worker)
                        worker_path = os.path.abspath("%s/../agent/%s" % (cwd, agent_worker))

                        worker_script_url = saga.Url("file://localhost/%s" % worker_path)

                        log_msg = "Copying '%s' to agent sandbox (%s)." % (worker_script_url, sandbox)
                        log_messages.append(log_msg)
                        logger.debug(log_msg)

                        worker_script = saga.filesystem.File(worker_script_url)
                        worker_script.copy("%s/agent-worker.py" % str(sandbox))
                        worker_script.close()

                    #########################################################
                    # now that the script is in place and we know where it is,
                    # we can launch the agent
                    if use_local_endpoints is True:
                        job_service_url = saga.Url(resource_cfg['local_job_manager_endpoint'])
                    else:
                        job_service_url = saga.Url(resource_cfg['remote_job_manager_endpoint'])

                    js = saga.job.Service(job_service_url, session=saga_session)

                    jd = saga.job.Description()
                    jd.working_directory = saga.Url(sandbox).path

                    bootstrap_args = "-r %s -d %s -s %s -p %s -t %s -c %s -V %s " %\
                        (database_host, database_name, session_uid, str(compute_pilot_id), runtime, number_cores, VERSION)

                    if 'pilot_agent_options' in resource_cfg and resource_cfg['pilot_agent_options'] is not None:
                        for option in resource_cfg['pilot_agent_options']:
                            bootstrap_args += " %s " % option

                    if 'python_interpreter' in resource_cfg and resource_cfg['python_interpreter'] is not None:
                        bootstrap_args += " -i %s " % resource_cfg['python_interpreter']
                    if 'pre_bootstrap' in resource_cfg and resource_cfg['pre_bootstrap'] is not None:
                        for command in resource_cfg['pre_bootstrap']:
                            bootstrap_args += " -e '%s' " % command

                    if cleanup is True: 
                        bootstrap_args += " -C "               # the cleanup flag    
                    if queue is not None:
                        bootstrap_args += " -q %s " % queue    # the queue name
                    if project is not None:
                        bootstrap_args += " -a %s " % project  # the project / allocation name

                    jd.executable = "/bin/bash"
                    jd.arguments = ["-l", "-c", '"./%s %s"' % (bootstrapper, bootstrap_args)]

                    logger.debug("Bootstrap command line: /bin/bash %s" % jd.arguments)

                    # fork:// and ssh:// don't support 'queue' and 'project'
                    if (job_service_url.schema != "fork") and (job_service_url.schema != "ssh"):

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

                    log_msg = "Submitting SAGA job with description: %s" % str(jd)
                    log_messages.append(log_msg)
                    logger.debug(log_msg)

                    pilotjob = js.create_job(jd)
                    pilotjob.run()

                    # do a quick error check
                    if pilotjob.state == saga.FAILED:
                        raise Exception("SAGA Job state was FAILED.")

                    saga_job_id = pilotjob.id
                    log_msg = "SAGA job submitted with job id %s" % str(saga_job_id)
                    log_messages.append(log_msg)
                    logger.debug(log_msg)

                    js.close()                    
                    ##
                    ##
                    ######################################################################

                    # Update the CU's state to 'DONE' if all transfers were successfull.
                    ts = datetime.datetime.utcnow()
                    pilot_col.update(
                        {"_id": ObjectId(compute_pilot_id)},
                        {"$set": {"state": PENDING_ACTIVE,
                                  "saga_job_id": saga_job_id},
                         "$push": {"statehistory": {"state": PENDING_ACTIVE, "timestamp": ts}},
                         "$pushAll": {"log": log_messages}}                    
                    )

                except Exception, ex:
                    # Update the CU's state 'FAILED'.
                    ts = datetime.datetime.utcnow()
                    log_messages = "Pilot launching failed: %s\n%s" % (str(ex), traceback.format_exc())
                    pilot_col.update(
                        {"_id": ObjectId(compute_pilot_id)},
                        {"$set": {"state": FAILED},
                         "$push": {"statehistory": {"state": FAILED, "timestamp": ts}},
                         "$push": {"log": log_messages}}
                    )
                    logger.error(log_messages)
