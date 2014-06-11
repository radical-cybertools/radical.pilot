"""
.. module:: radical.pilot.controller.pilot_launcher_worker
.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import bson
import datetime
import traceback
import threading

import weakref
from multiprocessing import Pool

from radical.utils import which

from radical.pilot.credentials import SSHCredential
from radical.pilot.utils.logger import logger

from radical.pilot.controller.pilot_launcher_worker import PilotLauncherWorker


# ----------------------------------------------------------------------------
#
class PilotManagerController(threading.Thread):
    """PilotManagerController is a threading worker that handles backend
       interaction for the PilotManager and Pilot classes.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, pilot_manager_uid, pilot_manager_data, 
        pilot_launcher_workers, resource_configurations, 
        db_connection, db_connection_info):
        """Le constructeur.
        """
        # The MongoDB database handle.
        self._db = db_connection

        # Multithreading stuff
        threading.Thread.__init__(self)
        self.daemon = True

        # Stop event can be set to terminate the main loop
        self._stop = threading.Event()
        self._stop.clear()

        # Initialized is set, once the run loop has pulled status
        # at least once. Other functions use it as a guard.
        self._initialized = threading.Event()
        self._initialized.clear()

        # Startup results contains a list of asynchronous startup results.
        self.startup_results = list()
        self.startup_results_lock = threading.Lock()

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

        # The manager-level callbacks.
        self._manager_callbacks = list()

        # The different command queues hold pending operations
        # that are passed to the worker. Command queues are inspected during
        # runtime in the run() loop and the worker acts upon them accordingly.
        #
        if pilot_manager_uid is None:
            # Try to register the PilotManager with the database.
            self._pm_id = self._db.insert_pilot_manager(
                pilot_manager_data=pilot_manager_data,
                pilot_launcher_workers=pilot_launcher_workers
            )
            self._num_pilot_launcher_workers = pilot_launcher_workers
        else:
            pm_json = self._db.get_pilot_manager(pilot_manager_id=pilot_manager_uid)
            self._pm_id = pilot_manager_uid
            self._num_pilot_launcher_workers = um_json["pilot_launcher_workers"]

        self.resource_configurations = resource_configurations

        # The pilot launcher worker(s) are autonomous processes that
        # execute pilot bootstrap / launcher requests concurrently.
        self._pilot_launcher_worker_pool = []
        for worker_number in range(1, self._num_pilot_launcher_workers+1):
            worker = PilotLauncherWorker(
                db_connection_info=db_connection_info, 
                pilot_manager_id=self._pm_id,
                resource_configurations=resource_configurations,
                number=worker_number
            )
            self._pilot_launcher_worker_pool.append(worker)
            worker.start()

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
        """List all known pilots.
        """
        return self._db.list_pilot_uids(self._pm_id)

    # ------------------------------------------------------------------------
    #
    def get_compute_pilot_data(self, pilot_uids=None):
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
           PilotManagerController.start().
        """
        logger.debug("Worker thread (ID: %s[%s]) for PilotManager %s started." %
                    (self.name, self.ident, self._pm_id))

        while not self._stop.is_set():

            # # Check if one or more startup requests have finished.
            # self.startup_results_lock.acquire()

            # new_startup_results = list()

            # for transfer_result in self.startup_results:
            #     if transfer_result.ready():
            #         result = transfer_result.get()

            #         self._db.update_pilot_state(
            #             pilot_uid=result["pilot_uid"],
            #             state=result["state"],
            #             sagajobid=result["saga_job_id"],
            #             sandbox=result["sandbox"],
            #             submitted=result["submitted"],
            #             logs=result["logs"]
            #         )

            #     else:
            #         new_startup_results.append(transfer_result)

            # self.startup_results = new_startup_results

            # self.startup_results_lock.release()

            # Check and update pilots. This needs to be optimized at
            # some point, i.e., state pulling should be conditional
            # or triggered by a tailable MongoDB cursor, etc.
            pilot_list = self._db.get_pilots(pilot_manager_id=self._pm_id)

            for pilot in pilot_list:
                pilot_id = str(pilot["_id"])

                new_state = pilot["state"]
                if pilot_id in self._shared_data:
                    old_state = self._shared_data[pilot_id]["data"]["state"]
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

            time.sleep(1)

        # shut down the autonomous pilot launcher worker(s)
        for worker in self._pilot_launcher_worker_pool:
            worker.terminate()
            worker.join()
            logger.debug("PilotManager.close(): %s terminated." % worker.name)

    # ------------------------------------------------------------------------
    #
    def register_start_pilot_request(self, pilot, resource_config, use_local_endpoints, session):
        """Register a new pilot start request with the worker.
        """

        saga_session = saga.Session()

        # Get the credentials from the session.
        cred_dict = []
        for cred in session.credentials:
            cred_dict.append(cred.as_dict())
            saga_session.add_context(cred._context)

        # create a new UID for the pilot
        pilot_uid = bson.ObjectId()


        # switch endpoint type
        if use_local_endpoints is True:
            filesystem_endpoint = resource_config['local_filesystem_endpoint']
        else:
            filesystem_endpoint = resource_config['remote_filesystem_endpoint']

        sandbox = pilot.description.sandbox
        fs = saga.Url(filesystem_endpoint)
        if sandbox is not None:
            fs.path = sandbox
        else:
            # No sandbox defined. try to determine
            found_dir_success = False

            if filesystem_endpoint.startswith("file"):
                workdir = os.path.expanduser("~")
                found_dir_success = True
            else:
                # get the home directory on the remote machine.
                # Note that this will only work for ssh or shell based access
                # mechanisms (FIXME)

                import saga.utils.pty_shell as sup

                url = "ssh://%s/" % fs.host
                shell = sup.PTYShell (url, saga_session, logger, opts={})

                ret, out, err = shell.run_sync (' echo "PWD: $PWD"')
                if  ret == 0 and 'PWD:' in out :
                    workdir = out.split(":")[1].strip()
                    logger.debug("Determined remote working directory for %s: '%s'" % (url, workdir))

                else :
                    error_msg = "Couldn't determine remote working directory."
                    logger.error(error_msg)
                    raise Exception(error_msg)

            # At this point we have determined 'pwd'
            fs.path = "%s/radical.pilot.sandbox" % workdir.rstrip()

        # This is the base URL / 'sandbox' for the pilot!
        agent_dir_url = saga.Url("%s/pilot-%s/" % (str(fs), str(pilot_uid)))

        # Create a database entry for the new pilot.
        pilot_uid, pilot_json = self._db.insert_pilot(
            pilot_uid=pilot_uid,
            pilot_manager_uid=self._pm_id,
            pilot_description=pilot.description,
            sandbox=str(agent_dir_url))

        # Create a shared data store entry
        self._shared_data[pilot_uid] = {
            'data':          pilot_json,
            'callbacks':     [],
            'facade_object': weakref.ref(pilot)
        }

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
            pilot.uid,
            self._shared_data[pilot_uid]["data"]["state"]
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
        self._db.signal_pilots(
            pilot_manager_id=self._pm_id,
            pilot_ids=pilot_ids, cmd="CANCEL")

        if pilot_ids is None:
            logger.info("Sent 'CANCEL' command to all pilots.")
        else:
            logger.info("Sent 'CANCEL' command to pilots %s.", pilot_ids)
