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
from radical.pilot.utils.logger import logger
from radical.pilot.credentials import SSHCredential

# BULK_LIMIT defines the max. number of transfer requests to pull from DB.
BULK_LIMIT=1

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

        while True:
            compute_pilot = None

            # See if we can find a ComputePilot that is waiting to be launched.
            ts = datetime.datetime.utcnow()
            compute_pilot = pilot_col.find_and_modify(
                query={"pilotmanager": self.pilot_manager_id,
                       "state" : PENDING_BOOTSTRAP},
                update={"$set" : {"state": BOOTSTRAPPING},
                        "$push": {"statehistory": {"state": BOOTSTRAPPING, "timestamp": ts}}},
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
                    cleanup      = compute_pilot['description']['cleanup']
                    pilot_agent  = compute_pilot['description']['pilot_agent_priv']

                    sandbox      = compute_pilot['sandbox']
                    resource_cfg = self.resource_configurations[compute_pilot['description']['resource']]

                    database_host = self.db_connection_info.url.split("://")[1], 
                    database_name = self.db_connection_info.dbname
                    session_uid   = self.db_connection_info.session_id

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

                    # Copy the bootstrap shell script
                    # This works for installed versions of RADICAL-Pilot
                    bs_script = which('bootstrap-and-run-agent')
                    if bs_script is None:
                        bs_script = os.path.abspath("%s/../../../../bin/bootstrap-and-run-agent" % os.path.dirname(os.path.abspath(__file__)))
                    # This works for non-installed versions (i.e., python setup.py test)
                    bs_script_url = saga.Url("file://localhost/%s" % bs_script)

                    log_msg = "Copying '%s' script to agent sandbox." % bs_script_url
                    log_messages.append(log_msg)
                    logger.debug(log_msg)

                    bs_script = saga.filesystem.File(bs_script_url)
                    bs_script.copy(saga.Url(sandbox))
                    bs_script.close()

                    # Copy the agent script
                    cwd = os.path.dirname(os.path.abspath(__file__))

                    if pilot_agent is not None:
                        logger.warning("Using custom pilot agent script: %s" % pilot_agent)
                        agent_path = os.path.abspath("%s/../agent/%s" % (cwd, pilot_agent))
                    else:
                        agent_path = os.path.abspath("%s/../agent/radical-pilot-agent.py" % cwd)

                    agent_script_url = saga.Url("file://localhost/%s" % agent_path)

                    log_msg = "Copying '%s' to agent sandbox." % agent_script_url
                    log_messages.append(log_msg)
                    logger.debug(log_msg)

                    agent_script = saga.filesystem.File(agent_script_url)
                    agent_script.copy("%s/radical-pilot-agent.py" % str(sandbox))
                    agent_script.close()

                    # now that the script is in place and we know where it is,
                    # we can launch the agent
                    js = saga.job.Service(resource_cfg['URL'], session=saga_session)

                    jd = saga.job.Description()
                    jd.working_directory = saga.Url(sandbox).path
                    jd.executable = "./bootstrap-and-run-agent"
                    jd.arguments = ["-r", database_host,   # database host (+ port)
                                    "-d", database_name,   # database name
                                    "-s", session_uid,     # session uid
                                    "-p", str(compute_pilot_id),  # pilot uid
                                    "-t", runtime,         # agent runtime in minutes
                                    "-c", number_cores] 

                    if cleanup is True:
                        jd.arguments.append("-C")

                    if 'task_launch_mode' in resource_cfg:
                        jd.arguments.extend(["-l", resource_cfg['task_launch_mode']])

                    # process the 'queue' attribute
                    if queue is not None:
                        jd.queue = queue
                    elif 'default_queue' in resource_cfg:
                        jd.queue = resource_cfg['default_queue']

                    # if resource config defines 'pre_bootstrap' commands,
                    # we add those to the argument list
                    if 'pre_bootstrap' in resource_cfg:
                        for command in resource_cfg['pre_bootstrap']:
                            jd.arguments.append("-e \"%s\"" % command)

                    # if resourc configuration defines a custom 'python_interpreter',
                    # we add it to the argument list
                    if 'python_interpreter' in resource_cfg:
                        jd.arguments.append(
                            "-i %s" % resource_cfg['python_interpreter'])

                    jd.output = "STDOUT"
                    jd.error = "STDERR"
                    jd.total_cpu_count = number_cores
                    jd.wall_time_limit = runtime

                    log_msg = "Starting SAGA job with description: %s" % str(jd)
                    log_messages.append(log_msg)
                    logger.debug(log_msg)

                    pilotjob = js.create_job(jd)
                    pilotjob.run()

                    saga_job_id = pilotjob.id

                    js.close()                    
                    ##
                    ##
                    ######################################################################

                    # Update the CU's state to 'DONE' if all transfers were successfull.
                    ts = datetime.datetime.utcnow()
                    pilot_col.update(
                        {"_id": ObjectId(compute_pilot_id)},
                        {"$set": {"state": PENDING_EXECUTION,
                                  "saga_job_id": saga_job_id},
                         "$push": {"statehistory": {"state": PENDING_EXECUTION, "timestamp": ts}},
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
