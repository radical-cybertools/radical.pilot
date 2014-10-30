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

from radical.pilot.states       import *
from radical.pilot.utils.logger import logger

from radical.pilot.controller.pilot_launcher_worker import PilotLauncherWorker

from radical.pilot.db.database import COMMAND_CANCEL_PILOT

# ----------------------------------------------------------------------------
#
class PilotManagerController(threading.Thread):
    """PilotManagerController is a threading worker that handles backend
       interaction for the PilotManager and Pilot classes.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, pilot_manager_uid, pilot_manager_data, 
        session, db_connection, db_connection_info, pilot_launcher_workers=1):
        """Le constructeur.
        """
        self._session = session

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
            self._num_pilot_launcher_workers = pm_json["pilot_launcher_workers"]

        # The pilot launcher worker(s) are autonomous processes that
        # execute pilot bootstrap / launcher requests concurrently.
        self._pilot_launcher_worker_pool = []
        for worker_number in range(1, self._num_pilot_launcher_workers+1):
            worker = PilotLauncherWorker(
                session=self._session,
                db_connection_info=db_connection_info, 
                pilot_manager_id=self._pm_id,
                number=worker_number
            )
            self._pilot_launcher_worker_pool.append(worker)
            worker.start()

        self._callback_histories = dict()

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
    def get_compute_pilot_data(self, pilot_ids=None):
        """Returns the raw data (json dicts) of one or more ComputePilots
           registered with this Worker / PilotManager
        """
        # Wait for the initialized event to assert proper operation.
        self._initialized.wait()

        try:
            if  pilot_ids is None:
                pilot_ids = self._shared_data.keys ()

            return_list_type = True
            if not isinstance(pilot_ids, list):
                return_list_type = False
                pilot_ids = [pilot_ids]

            data = list()
            for pilot_id in pilot_ids:
                data.append(self._shared_data[pilot_id]['data'])

            if  return_list_type :
                return data
            else :
                return data[0]

        except KeyError, ke:
            msg = "Unknown Pilot ID %s" % ke
            logger.error(msg)
            raise Exception(msg)

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

        # this is the point where, at the earliest, the application could have
        # been notified about pilot state changes.  So we record that event.
        if  not pilot_id in self._callback_histories :
            self._callback_histories[pilot_id] = list()
        self._callback_histories[pilot_id].append (
                {'timestamp' : datetime.datetime.utcnow(), 
                 'state'     : new_state})

        for cb in self._shared_data[pilot_id]['callbacks']:
            try:
                if  self._shared_data[pilot_id]['facade_object'] :
                    cb (self._shared_data[pilot_id]['facade_object'](), new_state)
                else :
                    logger.error("Couldn't call callback (no pilot instance)")
            except Exception, ex:
                logger.error("Couldn't call callback function %s" % str(ex))
                raise

        # If we have any manager-level callbacks registered, we
        # call those as well!
        for cb in self._manager_callbacks:
            try:
                if  self._shared_data[pilot_id]['facade_object'] :
                    cb(self._shared_data[pilot_id]['facade_object'](), new_state)
                else :
                    logger.error("Couldn't call manager callback (no pilot instance)")
            except Exception, ex:
                logger.error(
                    "Couldn't call callback function %s" % str(ex))
                raise

        # if we meet a final state, we record the object's callback history for
        # later evalutation
        if  new_state in (DONE, FAILED, CANCELED) :
            self._db.publish_compute_pilot_callback_history (pilot_id, self._callback_histories[pilot_id])
      # print 'publishing Callback history for %s' % pilot_id


    # ------------------------------------------------------------------------
    #
    def run(self):
        """run() is called when the process is started via
           PilotManagerController.start().
        """

        # make sure to catch sys.exit (which raises SystemExit)
        try :

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

                    self._shared_data[pilot_id]['data'] = pilot

                    if new_state != old_state:
                        # On a state change, we fire zee callbacks.
                        logger.info("ComputePilot '%s' state changed from '%s' to '%s'." % (pilot_id, old_state, new_state))

                        # The state of the pilot has changed, We call all
                        # pilot-level callbacks to propagate this.
                        self.call_callbacks(pilot_id, new_state)

                    # If the state is 'DONE', 'FAILED' or 'CANCELED', we also
                    # set the state of the compute unit accordingly
                    if new_state in [FAILED, DONE, CANCELED]:
                        self._db.set_all_running_compute_units(
                            pilot_id=pilot_id, 
                            state=CANCELED,
                            log="Pilot '%s' has terminated with state '%s'. CU canceled." % (pilot_id, new_state))

                # After the first iteration, we are officially initialized!
                if not self._initialized.is_set():
                    self._initialized.set()

                # sleep a little if this cycle was idle
                if  not len(pilot_list) :
                    time.sleep(1)

            # shut down the autonomous pilot launcher worker(s)
            for worker in self._pilot_launcher_worker_pool:
              # worker.terminate()
              # worker.join()
                logger.debug("PilotManager.close(): %s terminated." % worker.name)

        except SystemExit as e :
            logger.exception ("pilot manager controller thread caught system exit -- forcing application shutdown")
            import thread
            thread.interrupt_main ()
            

    # ------------------------------------------------------------------------
    #
    def register_start_pilot_request(self, pilot, resource_config):
        """Register a new pilot start request with the worker.
        """

        # create a new UID for the pilot
        pilot_uid = bson.ObjectId()

        # switch endpoint type
        filesystem_endpoint = resource_config['filesystem_endpoint']

        fs = saga.Url(filesystem_endpoint)

        # get the home directory on the remote machine.
        # Note that this will only work for (gsi)ssh or shell based access
        # mechanisms (FIXME)

        import saga.utils.pty_shell as sup

        if fs.port is not None:
            url = "%s://%s:%d/" % (fs.schema, fs.host, fs.port)
        else:
            url = "%s://%s/" % (fs.schema, fs.host)

        logger.debug ("saga.utils.PTYShell ('%s')" % url)
        shell = sup.PTYShell (url, self._session, logger, opts={})

        if pilot.description.sandbox is not None:
            workdir_raw = pilot.description.sandbox
        elif 'default_remote_workdir' in resource_config and \
            resource_config['default_remote_workdir'] is not None:
            workdir_raw = resource_config['default_remote_workdir']
        else:
            workdir_raw = "$PWD"

        ret, out, err = shell.run_sync (' echo "WORKDIR: %s"' % workdir_raw)
        if  ret == 0 and 'WORKDIR:' in out :
            workdir_expanded = out.split(":")[1].strip()
            logger.debug("Determined remote working directory for %s: '%s'" % (url, workdir_expanded))
        else :
            error_msg = "Couldn't determine remote working directory."
            logger.error(error_msg)
            raise Exception(error_msg)

        # At this point we have determined 'pwd'
        fs.path = "%s/radical.pilot.sandbox" % workdir_expanded

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
        if  self._shared_data[pilot_uid]['facade_object'] is None:
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

        if pilot_ids is None:
            self._db.send_command_to_pilot(COMMAND_CANCEL_PILOT, pilot_manager_id=self._pm_id)
            logger.info("Sent 'COMMAND_CANCEL_PILOT' command to all pilots.")
        else:
            self._db.send_command_to_pilot(COMMAND_CANCEL_PILOT, pilot_ids=pilot_ids)
            logger.info("Sent 'COMMAND_CANCEL_PILOT' command to pilots %s.", pilot_ids)
