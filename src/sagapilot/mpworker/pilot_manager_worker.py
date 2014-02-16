#pylint: disable=C0301, C0103, W0212

"""
.. module:: sinon.mpworker.pilot_manager_worker
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
import traceback
import multiprocessing
from Queue import Empty

from radical.utils import which

import sagapilot.states       as     states
from   sagapilot.credentials  import SSHCredential
from   sagapilot.utils.logger import logger

from bson import ObjectId

# ----------------------------------------------------------------------------
#
class PilotManagerWorker(multiprocessing.Process):
    """PilotManagerWorker is a multiprocessing worker that handles backend 
       interaction for the PilotManager and Pilot classes.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, pilot_manager_uid, pilot_manager_data, db_connection):
        """Le constructeur.
        """

        # Multiprocessing stuff
        multiprocessing.Process.__init__(self)
        self.daemon  = True

        # Stop event can be set to terminate the main loop
        self._stop   = multiprocessing.Event()
        self._stop.clear()

        # The shard_data_manager handles data exchange between the worker
        # process and the API objects. The communication is unidirectional:
        # workers WRITE to _shared_data and API methods READ from _shared_data.
        # The strucuture of _shared_data is as follows:
        #
        # { pilot1_uid: MongoDB document (dict),
        #   pilot2_uid: MongoDB document (dict),
        #   ...
        # }
        #
        shard_data_manager = multiprocessing.Manager()
        self._shared_data = shard_data_manager.dict()

        # The callback dictionary. The structure is as follows:
        #
        # { pilot1_uid : [func_ptr, func_ptr, func_ptr, ...],
        #   pilot2_uid : [func_ptr, func_ptr, func_ptr, ...],
        #   ...
        # }
        #
        self._callbacks = shard_data_manager.dict()

        # The MongoDB database handle.
        self._db = db_connection

        # The different command queues hold pending operations
        # that are passed to the worker. Command queues are inspected during
        # runtime in the run() loop and the worker acts upon them accordingly.
        self._cancel_pilot_requests = multiprocessing.Queue()
        self._startup_pilot_requests = multiprocessing.Queue()

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
    def stop(self):
        """stop() signals the process to finish up and terminate.
        """
        self._stop.set()
        self.join()
        logger.info("Worker process (PID: %s) for PilotManager %s stopped." % (self.pid, self._pm_id))

    # ------------------------------------------------------------------------
    #
    def run(self):
        """run() is called when the process is started via
           PilotManagerWorker.start().
        """
        logger.info("Worker process for PilotManager %s started with PID %s." % (self._pm_id, self.pid))

        while not self._stop.is_set():

            # Check if there are any pilots to cancel.
            try:
                pilot_ids = self._cancel_pilot_requests.get_nowait()
                self._execute_cancel_pilots(pilot_ids)
            except Empty:
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

            except Empty:
                pass

            # Check and update pilots. This needs to be optimized at
            # some point, i.e., state pulling should be conditional
            # or triggered by a tailable MongoDB cursor, etc.
            pilot_list = self._db.get_pilots(pilot_manager_id=self._pm_id)

            for pilot in pilot_list:
                pilot_id = str(pilot["_id"])

                new_state = pilot["info"]["state"]
                if pilot_id in self._shared_data:
                    old_state = self._shared_data[pilot_id]["info"]["state"]
                else:
                    old_state = None

                if new_state != old_state:
                    # On a state change, we fire zee callbacks.
                    logger.info("ComputePilot '%s' state changed from '%s' to '%s'." % (pilot_id, old_state, new_state))
                    if pilot_id in self._callbacks:
                        for cb in self._callbacks[pilot_id]:
                            cb(pilot_id, new_state)

                self._shared_data[pilot_id] = pilot

            time.sleep(1)

            #for cb in self._callbacks["XXX"]:
            #    cb("ISDDDD", "BLARGHHHH")

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
            logger.info("Sent 'CANCEL' to all pilots.")
        else:
            logger.info("Sent 'CANCEL' to pilots %s.", pilot_ids)

    # ------------------------------------------------------------------------
    #
    def _execute_startup_pilot(self, pilot_uid, pilot_description, resource_cfg, session, credentials):
        """Carries out pilot cancelation.
        """
        #resource_key = pilot_description['description']['Resource']
        number_cores = pilot_description['Cores']
        runtime      = pilot_description['Runtime']
        queue        = pilot_description['Queue']
        sandbox      = pilot_description['Sandbox']

        # At the end of the submission attempt, pilot_logs will contain
        # all log messages.
        pilot_logs    = []

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

                logger.info("Added credential %s to SAGA job service." % str(cred))

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
                    # A horrible hack to get the home directory on the remote machine
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

                        p = subprocess.Popen(["ssh", url,  "pwd"], 
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        workdir, err = p.communicate()

                        if err != "":
                            logger.warning("Couldn't determine remote working directory for %s: %s" % (url, err))
                        else:
                            logger.info("Determined remote working directory for %s: %s" % (url, workdir))
                            found_dir_success = True
                            break 
                    
                if found_dir_success == False:
                    raise Exception("Couldn't determine remote working directory.")

                # At this point we have determined 'pwd'
                fs.path += "%s/sagapilot.sandbox" % workdir.rstrip()

            agent_dir_url = saga.Url("%s/pilot-%s/" \
                % (str(fs), str(pilot_uid)))

            agent_dir = saga.filesystem.Directory(agent_dir_url, 
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
            session_uid   = session["uid"]

            # now that the script is in place and we know where it is,
            # we can launch the agent
            js = saga.job.Service(resource_cfg['URL'], session=saga_session)

            jd = saga.job.Description()
            jd.working_directory = agent_dir_url.path
            jd.executable        = "./bootstrap-and-run-agent"
            jd.arguments         = ["-r", database_host,  # database host (+ port)
                                    "-d", database_name,  # database name
                                    "-s", session_uid,    # session uid
                                    "-p", str(pilot_uid),  # pilot uid
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
            logger.info(log_msg)

            # Submission was successful. We can set the pilot state to 'PENDING'.
            self._db.update_pilot_state(pilot_uid=str(pilot_uid),
                state=states.PENDING, sagajobid=pilotjob_id,
                submitted=datetime.datetime.utcnow(), logs=pilot_logs)

        except Exception, ex:
            error_msg = "Pilot Job submission failed:\n %s" % (traceback.format_exc())
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
    def get_compute_pilot_data(self, pilot_uid):
        """Retruns the raw data (json dicts) of one or more ComputePilots 
           registered with this Worker / PilotManager
        """
        return self._shared_data[pilot_uid]

    # ------------------------------------------------------------------------
    #
    def register_start_pilot_request(self, pilot_description, resource_config, session):
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
            pilot_description=pilot_description)

        # Create a shared data store entry
        self._shared_data[pilot_uid] = pilot_json

        # Add the startup request to the request queue.
        self._startup_pilot_requests.put(
            {"pilot_uid": pilot_uid,
             "pilot_description": pilot_description.as_dict(),
             "resource_config": resource_config,
             "session": session.as_dict(),
             "credentials": cred_dict})

        return pilot_uid

    # ------------------------------------------------------------------------
    #
    def register_pilot_state_callback(self, pilot_uid, callback_func):
        """Registers a callback function.
        """
        if pilot_uid not in self._callbacks:
            # First callback ever registered for pilot_uid.
            self._callbacks[pilot_uid] = [callback_func]
        else:
            # Additional callback for pilot_uid.
            self._callbacks[pilot_uid].append(callback_func)

        # Callbacks can only be registered when the ComputeAlready has a
        # state. To address this shortcomming we call the callback with the
        # current ComputePilot state as soon as it is registered.
        callback_func(pilot_uid, self._shared_data[pilot_uid]["info"]["state"])

    # ------------------------------------------------------------------------
    #
    def register_cancel_pilots_request(self, pilot_ids):
        """Registers one or more pilots for cancelation.
        """
        self._cancel_pilot_requests.put(pilot_ids)


