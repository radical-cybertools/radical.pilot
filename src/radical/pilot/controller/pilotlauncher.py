"""
.. module:: radical.pilot.controller.pilotlauncher
   :platform: Unix
   :synopsis: Implements the pilot laumcher functionality.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import datetime
import traceback

from radical.utils import which

from radical.pilot import states
from radical.pilot.credentials import SSHCredential
from radical.pilot.utils.logger import logger


# ------------------------------------------------------------------------
#
def launch_pilot(pilot_uid, pilot_description,
                 resource_cfg, agent_dir_url, session_dict, credentials_dict):
    """launch_pilot() is a self contained function that launches a RADICAL-Pilot
    agent on a local or remote machine according to the provided specification.

    This function is called asynchronously and attached to one of the
    PilotManager's worker processes. To maintain pickleability et al., no
    facade objects etc. go in an out of this function, just plain dictionaries.
    """

    agent_dir_url = saga.Url(agent_dir_url)

    #resource_key = pilot_description['description']['Resource']
    number_cores = pilot_description['cores']
    runtime = pilot_description['runtime']
    queue = pilot_description['queue']
    sandbox = pilot_description['sandbox']
    cleanup = pilot_description['cleanup']

    pilot_agent = pilot_description['pilot_agent_priv']

    # At the end of the submission attempt, pilot_logs will contain
    # all log messages.
    pilot_logs = []

    ########################################################
    # Create SAGA Job description and submit the pilot job #
    ########################################################
    try:
        # create a custom SAGA Session and add the credentials
        # that are attached to the session
        saga_session = saga.Session()

        for cred_dict in credentials_dict:
            cred = SSHCredential.from_dict(cred_dict)

            saga_session.add_context(cred._context)

            logger.debug("Added credential %s to SAGA job service." % str(cred))

        agent_dir = saga.filesystem.Directory(
            agent_dir_url,
            saga.filesystem.CREATE_PARENTS, session=saga_session)

        agent_dir.close()

        log_msg = "Created agent sandbox '%s'." % str(agent_dir_url)
        pilot_logs.append(log_msg)
        logger.debug(log_msg)

        # Copy the bootstrap shell script
        # This works for installed versions of RADICAL-Pilot
        bs_script = which('bootstrap-and-run-agent')
        if bs_script is None:
            bs_script = os.path.abspath("%s/../../../../bin/bootstrap-and-run-agent" % os.path.dirname(os.path.abspath(__file__)))
        # This works for non-installed versions (i.e., python setup.py test)
        bs_script_url = saga.Url("file://localhost/%s" % bs_script)

        log_msg = "Copying '%s' script to agent sandbox." % bs_script_url
        pilot_logs.append(log_msg)
        logger.debug(log_msg)

        bs_script = saga.filesystem.File(bs_script_url)
        bs_script.copy(agent_dir_url)
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
        pilot_logs.append(log_msg)
        logger.debug(log_msg)

        agent_script = saga.filesystem.File(agent_script_url)
        agent_script.copy("%s/radical-pilot-agent.py" % str(agent_dir_url))
        agent_script.close()

        # extract the required connection parameters and uids
        # for the agent:
        database_host = session_dict["database_url"].split("://")[1]
        database_name = session_dict["database_name"]
        session_uid = session_dict["uid"]

        # now that the script is in place and we know where it is,
        # we can launch the agent
        js = saga.job.Service(resource_cfg['URL'], session=saga_session)

        jd = saga.job.Description()
        jd.working_directory = agent_dir_url.path
        jd.executable = "./bootstrap-and-run-agent"
        jd.arguments = ["-r", database_host,   # database host (+ port)
                        "-d", database_name,   # database name
                        "-s", session_uid,     # session uid
                        "-p", str(pilot_uid),  # pilot uid
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

        logger.debug("Starting pilot agent with description: %s" % str(jd))

        pilotjob = js.create_job(jd)
        pilotjob.run()

        pilotjob_id = pilotjob.id

        js.close()

        log_msg = "ComputePilot agent successfully submitted with JobID '%s'" % pilotjob_id
        pilot_logs.append(log_msg)
        logger.info(log_msg)

        # Submission was successful. We can set the pilot state to 'PENDING'.

        result = {
            "pilot_uid":   str(pilot_uid),
            "saga_job_id": pilotjob_id,
            "state":       states.PENDING,
            "sandbox":     str(agent_dir_url),
            "submitted":   datetime.datetime.utcnow(),
            "logs":        pilot_logs
        }
        return result

    except Exception, ex:
        error_msg = "Pilot Job submission failed:\n %s" % str(ex)
        pilot_logs.append(error_msg)
        logger.error(error_msg)

        result = {
            "pilot_uid":   str(pilot_uid),
            "saga_job_id": None,
            "state":       states.FAILED,
            "sandbox":     str(agent_dir_url),
            "submitted":   datetime.datetime.utcnow(),
            "logs":        pilot_logs
        }
        return result
