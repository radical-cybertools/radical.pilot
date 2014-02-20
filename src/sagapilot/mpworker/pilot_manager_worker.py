#pylint: disable=C0301, C0103, W0212

"""
.. module:: sinon.mpworker.pilot_manager_worker
   :platform: Unix
   :synopsis: Implements a multiprocessing worker backend for
              the PilotManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

#pylint: disable=C0301, C0103
#pylint: disable=W0212

import os
import time
import saga
import datetime
import traceback
import multiprocessing
import threading
import Queue  # import Empty

import weakref

from radical.utils import which

import sagapilot.states as states
from sagapilot.credentials import SSHCredential
from sagapilot.utils.logger import logger


# ----------------------------------------------------------------------------
#
class PilotManagerWorker(threading.Thread):
    """PilotManagerWorker is a threading worker that handles backend
       interaction for the PilotManager and Pilot classes.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, pilot_manager_uid, pilot_manager_data, db_connection):
        """Le constructeur.
        """

        # Multithreading stuff
        threading.Thread.__init__(self)
        self.daemon = True
        self.name = 'PMWThread'

        # Stop event can be set to terminate the main loop
        self._stop = threading.Event()
        self._stop.clear()

        # Initialized is set, once the run loop has pulled status
        # at least once. Other functions use it as a guard.
        self._initialized = threading.Event()
        self._initialized.clear()

        # The shard_data_manager handles data exchange between the worker
        # process and the API objects. The communication is unidirectional:
        # workers WRITE to _shared_data and API methods READ from _shared_data.
        # The strucuture of _shared_data is as follows:
        #
        #  self._shared_data[pilot_uid] = {
        #      'data':          pilot_json,
        #      'callbacks':     []
        #      'facade_object': None
        #  }
        #
        self._shared_data = dict()

        # The manager-level list.
        #
        self._manager_callbacks = list()

        # The MongoDB database handle.
        #
        self._db = db_connection

        # The different command queues hold pending operations
        # that are passed to the worker. Command queues are inspected during
        # runtime in the run() loop and the worker acts upon them accordingly.
        #
        self._cancel_pilot_requests = Queue.Queue()
        self._startup_pilot_requests = Queue.Queue()

        if pilot_manager_uid is None:
            # Try to register the PilotManager with the database.
            self._pm_id = self._db.insert_pilot_manager(
                pilot_manager_data=pilot_manager_data
            )
        else:
            self._pm_id = pilot_manager_uid

    # ------------------------------------------------------------------------
    #
    @classmethod
    def uid_exists(cls, db_connection, pilot_manager_uid):
        """Checks wether a pilot unit manager UID exists.
        """
        exists = False

        if pilot_manager_uid in db_connection.list_pilot_manager_uids():
            exists = True

        return exists

    # ------------------------------------------------------------------------
    #
    @property
    def pilot_manager_uid(self):
        """Returns the uid of the associated PilotMangager
        """
        return self._pm_id

    # ------------------------------------------------------------------------
    #
    def list_pilots(self):
        """List all known p[ilots.
        """
        return self._db.list_pilot_uids(self._pm_id)

    # ------------------------------------------------------------------------
    #
    def get_compute_pilot_data(self, pilot_uids):
        """Returns the raw data (json dicts) of one or more ComputePilots
           registered with this Worker / PilotManager
        """
        # Wait for the initialized event to assert proper operation.
        self._initialized.wait()

        if pilot_uids is None:
            data = self._db.get_pilots(pilot_manager_id=self._pm_id)
            return data

        else:
            if not isinstance(pilot_uids, list):
                return self._shared_data[pilot_uids]['data']
            else:
                data = list()
                for pilot_uid in pilot_uids:
                    data.append(self._shared_data[pilot_uid]['data'])
                return data

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """stop() signals the process to finish up and terminate.
        """
        self._stop.set()
        self.join()
        logger.debug("Worker thread (ID: %s[%s]) for PilotManager %s stopped." %
                    (self.name, self.ident, self._pm_id))

    # ------------------------------------------------------------------------
    #
    def call_callbacks(self, pilot_id, new_state):
        """Wrapper function to call all all relevant callbacks, on pilot-level
        as well as manager-level.
        """

        for cb in self._shared_data[pilot_id]['callbacks']:
            try:
                cb(self._shared_data[pilot_id]['facade_object'](),
                   new_state)
            except Exception, ex:
                logger.error(
                    "Couldn't call callback function %s" % str(ex))

        # If we have any manager-level callbacks registered, we
        # call those as well!
        for cb in self._manager_callbacks:
            try:
                cb(self._shared_data[pilot_id]['facade_object'](),
                   new_state)
            except Exception, ex:
                logger.error(
                    "Couldn't call callback function %s" % str(ex))

    # ------------------------------------------------------------------------
    #
    def run(self):
        """run() is called when the process is started via
           PilotManagerWorker.start().
        """
        logger.debug("Worker thread (ID: %s[%s]) for PilotManager %s started." %
                    (self.name, self.ident, self._pm_id))

        while not self._stop.is_set():

            # Check if there are any pilots to cancel.
            try:
                pilot_ids = self._cancel_pilot_requests.get_nowait()
                self._execute_cancel_pilots(pilot_ids)
            except Queue.Empty:
                pass

            # Check if there are any pilots to start.
            try:
                request = self._startup_pilot_requests.get_nowait()
                self._execute_startup_pilot(
                    request["pilot_uid"],
                    request["pilot_description"],
                    request["resource_config"],
                    request["session"],
                    request["credentials"])

            except Queue.Empty:
                pass

            # Check and update pilots. This needs to be optimized at
            # some point, i.e., state pulling should be conditional
            # or triggered by a tailable MongoDB cursor, etc.
            pilot_list = self._db.get_pilots(pilot_manager_id=self._pm_id)

            for pilot in pilot_list:
                pilot_id = str(pilot["_id"])

                new_state = pilot["info"]["state"]
                if pilot_id in self._shared_data:
                    old_state = self._shared_data[pilot_id]["data"]["info"]["state"]
                else:
                    old_state = None
                    self._shared_data[pilot_id] = {
                        'data':          pilot,
                        'callbacks':     [],
                        'facade_object': None
                    }

                if new_state != old_state:
                    # On a state change, we fire zee callbacks.
                    logger.info("ComputePilot '%s' state changed from '%s' to '%s'." % (pilot_id, old_state, new_state))

                    # The state of the pilot has changed, We call all
                    # pilot-level callbacks to propagate this.
                    self.call_callbacks(pilot_id, new_state)

                self._shared_data[pilot_id]['data'] = pilot

                # After the first iteration, we are officially initialized!
                if not self._initialized.is_set():
                    self._initialized.set()
                    logger.debug("Worker status set to 'initialized'.")

            time.sleep(1)

    # ------------------------------------------------------------------------
    #
    def _execute_cancel_pilots(self, pilot_ids):
        """Carries out pilot cancelation.
        """
        self._db.signal_pilots(
            pilot_manager_id=self._pm_id,
            pilot_ids=pilot_ids, cmd="CANCEL")

        if pilot_ids is None:
            logger.info("Sent 'CANCEL' to all pilots.")
        else:
            logger.info("Sent 'CANCEL' to pilots %s.", pilot_ids)

    # ------------------------------------------------------------------------
    #
    def _execute_startup_pilot(self, pilot_uid, pilot_description,
                               resource_cfg, session, credentials):
        """Carries out pilot cancelation.
        """
        #resource_key = pilot_description['description']['Resource']
        number_cores = pilot_description['Cores']
        runtime = pilot_description['Runtime']
        queue = pilot_description['Queue']
        sandbox = pilot_description['Sandbox']

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

            for cred_dict in credentials:
                cred = SSHCredential.from_dict(cred_dict)

                saga_session.add_context(cred._context)

                logger.debug("Added credential %s to SAGA job service." % str(cred))

            # Create working directory if it doesn't exist and copy
            # the agent bootstrap script into it.
            #
            # We create a new sub-driectory for each agent. each
            # agent will bootstrap its own virtual environment in this
            # directory.
            #
            fs = saga.Url(resource_cfg['filesystem'])
            if sandbox is not None:
                fs.path += sandbox
            else:
                # No sandbox defined. try to determine
                found_dir_success = False

                if resource_cfg['filesystem'].startswith("file"):
                    workdir = os.path.expanduser("~")
                    found_dir_success = True
                else:
                    # A horrible hack to get the home directory on the
                    # remote machine.
                    import subprocess

                    usernames = [None]
                    for cred in credentials:
                        usernames.append(cred["user_id"])

                    # We have mutliple usernames we can try... :/
                    for username in usernames:
                        if username is not None:
                            url = "%s@%s" % (username, fs.host)
                        else:
                            url = fs.host

                        p = subprocess.Popen(
                            ["ssh", url,  "pwd"],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE
                        )
                        workdir, err = p.communicate()

                        if err != "":
                            logger.warning("Couldn't determine remote working directory for %s: %s" % (url, err))
                        else:
                            logger.debug("Determined remote working directory for %s: %s" % (url, workdir))
                            found_dir_success = True
                            break

                if found_dir_success is False:
                    error_msg = "Couldn't determine remote working directory."
                    logger.error(error_msg)
                    raise Exception(error_msg)

                # At this point we have determined 'pwd'
                fs.path += "%s/sagapilot.sandbox" % workdir.rstrip()

            agent_dir_url = saga.Url("%s/pilot-%s/" % (str(fs), str(pilot_uid)))

            agent_dir = saga.filesystem.Directory(
                agent_dir_url,
                saga.filesystem.CREATE_PARENTS)

            log_msg = "Created agent directory '%s'." % str(agent_dir_url)
            pilot_logs.append(log_msg)
            logger.debug(log_msg)

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
            logger.debug(log_msg)

            # Copy the agent script
            cwd = os.path.dirname(os.path.abspath(__file__))
            agent_path = os.path.abspath("%s/../agent/sagapilot-agent.py" % cwd)
            agent_script_url = saga.Url("file://localhost/%s" % agent_path)
            agent_script = saga.filesystem.File(agent_script_url)
            agent_script.copy(agent_dir_url)

            log_msg = "Copied '%s' script to agent directory." % agent_script_url
            pilot_logs.append(log_msg)
            logger.debug(log_msg)

            # extract the required connection parameters and uids
            # for the agent:
            database_host = session["database_url"].split("://")[1]
            database_name = session["database_name"]
            session_uid = session["uid"]

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
                            "-c", number_cores,    # number of cores
                            "-C"]                  # clean up by default

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
            logger.info(log_msg)

            # Submission was successful. We can set the pilot state to 'PENDING'.
            self._db.update_pilot_state(
                pilot_uid=str(pilot_uid),
                state=states.PENDING, sagajobid=pilotjob_id,
                submitted=datetime.datetime.utcnow(), logs=pilot_logs)

        except Exception, ex:
            error_msg = "Pilot Job submission failed:\n %s" % (
                traceback.format_exc())

            pilot_logs.append(error_msg)
            logger.error(error_msg)

            # Submission wasn't successful. Update the pilot's state
            # to 'FAILED'.
            now = datetime.datetime.utcnow()
            self._db.update_pilot_state(pilot_uid=str(pilot_uid),
                                        state=states.FAILED,
                                        submitted=now,
                                        logs=pilot_logs)

    # ------------------------------------------------------------------------
    #
    def register_start_pilot_request(self, pilot, resource_config, session):
        """Register a new pilot start request with the worker.
        """

        # Get the credentials from the session.
        cred_dict = []
        for cred in session.credentials:
            cred_dict.append(cred.as_dict())

        # Convert the session to dict.
        session_dict = session.as_dict()

        # Create a database entry for the new pilot.
        pilot_uid, pilot_json = self._db.insert_pilot(
            pilot_manager_uid=self._pm_id,
            pilot_description=pilot.description)

        # Create a shared data store entry
        self._shared_data[pilot_uid] = {
            'data':          pilot_json,
            'callbacks':     [],
            'facade_object': weakref.ref(pilot)
        }

        # Add the startup request to the request queue.
        self._startup_pilot_requests.put(
            {"pilot_uid":         pilot_uid,
             "pilot_description": pilot.description.as_dict(),
             "resource_config":   resource_config,
             "session":           session.as_dict(),
             "credentials":       cred_dict})

        return pilot_uid

    # ------------------------------------------------------------------------
    #
    def register_pilot_callback(self, pilot, callback_func):
        """Registers a callback function.
        """
        pilot_uid = pilot.uid
        self._shared_data[pilot_uid]['callbacks'].append(callback_func)

        # Add the facade object if missing, e.g., after a re-connect.
        if self._shared_data[pilot_uid]['facade_object'] is None:
            self._shared_data[pilot_uid]['facade_object'] = weakref.ref(pilot)

        # Callbacks can only be registered when the ComputeAlready has a
        # state. To partially address this shortcomming we call the callback
        # with the current ComputePilot state as soon as it is registered.
        self.call_callbacks(
            pilot,
            self._shared_data[pilot_uid]["data"]["info"]["state"]
        )

    # ------------------------------------------------------------------------
    #
    def register_manager_callback(self, callback_func):
        """Registers a manager-level callback.
        """
        self._manager_callbacks.append(callback_func)

    # ------------------------------------------------------------------------
    #
    def register_cancel_pilots_request(self, pilot_ids):
        """Registers one or more pilots for cancelation.
        """
        self._cancel_pilot_requests.put(pilot_ids)
