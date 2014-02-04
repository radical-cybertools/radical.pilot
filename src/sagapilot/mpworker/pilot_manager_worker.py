"""
.. module:: sinon.mpworker.PilotManagerWorker
   :platform: Unix
   :synopsis: Implements a multiprocessing worker backend for 
              the PilotManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

#pylint: disable=C0301, C0103
#pylint: disable=W0212

import os
import time
import saga
import datetime
import multiprocessing
from Queue import Empty


from radical.utils import which

import sagapilot.states     as states
import sagapilot.exceptions as exceptions


# ----------------------------------------------------------------------------
#
class PilotManagerWorker(multiprocessing.Process):
    """PilotManagerWorker is a multiprocessing worker that handles backend 
       interaction for the PilotManager and Pilot classes.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, logger, pilotmanager_id, db_connection):

        # Multiprocessing stuff
        multiprocessing.Process.__init__(self)
        self.daemon  = True

        self._stop   = multiprocessing.Event()
        self._stop.clear()

        self.logger  = logger
        self._pm_id  = pilotmanager_id
        self._db     = db_connection

        # The different command queues hold pending operations
        # that are passed to the worker. Command queues are inspected during 
        # runtime in the run() loop and the worker acts upon them accordingly. 
        self._cancel_pilot_requests  = multiprocessing.Queue()
        self._startup_pilot_requests = multiprocessing.Queue()

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """stop() signals the process to finish up and terminate. 
        """
        self._stop.set()
        self.join()
        self.logger.info("Worker process (PID: %s) for PilotManager %s stopped." % (self.pid, self._pm_id))

    # ------------------------------------------------------------------------
    #
    def run(self):
        """run() is called when the process is started via 
           PilotManagerWorker.start().
        """
        self.logger.info("Worker process for PilotManager %s started with PID %s." % (self._pm_id, self.pid))

        while not self._stop.is_set():

            # Check if there are any pilots to cancel.
            try:
                pilot_ids = self._cancel_pilot_requests.get_nowait()
                self._execute_cancel_pilots(pilot_ids)
            except Empty:
                pass

            # Check if there are any pilots to start.
            try:
                pilot_descriptions = self._startup_pilot_requests.get_nowait()
                self._execute_startup_pilots(pilot_descriptions)
            except Empty:
                pass
            # TODO: catch DB error HERE
            time.sleep(1)

    # ------------------------------------------------------------------------
    #
    def list_pilots(self):
        """List all known p[ilots.
        """
        return self._db.list_pilot_uids(self._pm_id)

    # ------------------------------------------------------------------------
    #
    def _execute_cancel_pilots(self, pilot_ids):
        """Carries out pilot cancelation.
        """
        self._db.signal_pilots(pilot_manager_id=self._pm_id, 
            pilot_ids=pilot_ids, cmd="CANCEL")

        if pilot_ids is None:
            self.logger.info("Sent 'CANCEL' to all pilots.")
        else:
            self.logger.info("Sent 'CANCEL' to pilots %s.", pilot_ids)

    # ------------------------------------------------------------------------
    #
    def _execute_startup_pilots(self, pilot_descriptions):
        """Carries out pilot cancelation.
        """
        for pilot_id, pilot_description in pilot_descriptions.iteritems():
            #resource_key = pilot_description['description']['Resource']
            number_cores = pilot_description['description']['Cores']
            runtime      = pilot_description['description']['Runtime']
            queue        = pilot_description['description']['Queue']
            sandbox      = pilot_description['description']['Sandbox']

            session      = pilot_description['session']
            resource_cfg = pilot_description['resourcecfg']

            # At the end of the submission attempt, pilot_logs will contain
            # all log messages.
            pilot_logs    = []

            ########################################################
            # Create SAGA Job description and submit the pilot job #
            ########################################################
            try:
                # create a custom SAGA Session and add the credentials
                # that are attached to the session
                #session = saga.Session()
                #for cred in self._session.list_credentials():
                #    ctx = cred._context
                #    session.add_context(ctx)
                #    logger.info("Added credential %s to SAGA job service." % str(cred))

                # Create working directory if it doesn't exist and copy
                # the agent bootstrap script into it. 
                #
                # We create a new sub-driectory for each agent. each 
                # agent will bootstrap its own virtual environment in this 
                # directory.
                #
                fs = saga.Url(resource_cfg['filesystem'])
                fs.path += sandbox

                agent_dir_url = saga.Url("%s/pilot-%s/" \
                    % (str(fs), str(pilot_id)))

                agent_dir = saga.filesystem.Directory(agent_dir_url, 
                    saga.filesystem.CREATE_PARENTS)

                log_msg = "Created agent directory '%s'." % str(agent_dir_url)
                pilot_logs.append(log_msg)
                self.logger.debug(log_msg)

                # Copy the bootstrap shell script
                # This works for installed versions of saga-pilot
                bs_script = which('bootstrap-and-run-agent')
                if bs_script is None:
                    bs_script = os.path.abspath("%s/../../../bin/bootstrap-and-run-agent" % os.path.dirname(os.path.abspath(__file__)))
                # This works for non-installed versions (i.e., python setup.py test)
                bs_script_url = saga.Url("file://localhost/%s" % bs_script) 

                bs_script = saga.filesystem.File(bs_script_url)
                bs_script.copy(agent_dir_url)

                log_msg = "Copied '%s' script to agent directory." % bs_script_url
                pilot_logs.append(log_msg)
                self.logger.debug(log_msg)

                # Copy the agent script
                cwd = os.path.dirname(os.path.abspath(__file__))
                agent_path = os.path.abspath("%s/../agent/sagapilot-agent.py" % cwd)
                agent_script_url = saga.Url("file://localhost/%s" % agent_path) 
                agent_script = saga.filesystem.File(agent_script_url)
                agent_script.copy(agent_dir_url)

                log_msg = "Copied '%s' script to agent directory." % agent_script_url
                pilot_logs.append(log_msg)
                self.logger.debug(log_msg)

                # extract the required connection parameters and uids
                # for the agent:
                database_host = session["database_url"].split("://")[1]
                database_name = session["database_name"]
                session_uid   = session["uid"]

                # now that the script is in place and we know where it is,
                # we can launch the agent
                js = saga.job.Service(resource_cfg['URL'], session=session)

                jd = saga.job.Description()
                jd.working_directory = agent_dir_url.path
                jd.executable        = "./bootstrap-and-run-agent"
                jd.arguments         = ["-r", database_host,  # database host (+ port)
                                        "-d", database_name,  # database name
                                        "-s", session_uid,    # session uid
                                        "-p", str(pilot_id),  # pilot uid
                                        "-t", runtime,       # agent runtime in minutes
                                        "-c", number_cores,   # number of cores
                                        "-C"]                 # clean up by default

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
                    jd.arguments.append("-i %s" % resource_cfg['python_interpreter'])

                jd.output            = "STDOUT"
                jd.error             = "STDERR"
                jd.total_cpu_count   = number_cores
                jd.wall_time_limit    = runtime

                pilotjob = js.create_job(jd)
                pilotjob.run()

                pilotjob_id = pilotjob.id

                # clean up / close all saga objects
                js.close()
                agent_dir.close()
                agent_script.close()
                bs_script.close()

                log_msg = "ComputePilot agent successfully submitted with JobID '%s'" % pilotjob_id
                pilot_logs.append(log_msg)
                self.logger.info(log_msg)

                self._db.update_pilot_state(pilot_uid=str(pilot_id),
                    state=states.PENDING, sagajobid=pilotjob_id,
                    submitted=datetime.datetime.utcnow(), logs=pilot_logs)

            except Exception, ex:
                error_msg = "Pilot Job submission failed: '%s'" % str(ex)
                pilot_description['info']['state'] = states.FAILED
                pilot_logs.append(error_msg)
                self.logger.error(error_msg)

                self._db.update_pilot_state(pilot_uid=str(pilot_id),
                    state=states.FAILED, submitted=datetime.datetime.utcnow(), logs=pilot_logs)

    # ------------------------------------------------------------------------
    #
    def register_startup_pilots_request(self, pilot_descriptions):
        """Registers one or more pilots for cancelation.
        """
        self._startup_pilot_requests.put(pilot_descriptions)
        self._db.insert_new_pilots(pilot_manager_uid=self._pm_id, pilot_descriptions=pilot_descriptions)


    # ------------------------------------------------------------------------
    #
    def register_cancel_pilots_request(self, pilot_ids):
        """Registers one or more pilots for cancelation.
        """
        self._cancel_pilot_requests.put(pilot_ids)


