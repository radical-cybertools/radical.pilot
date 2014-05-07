"""
.. module:: radical.pilot.controller.pilot_launcher_worker
.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import Queue
import weakref
import threading

from multiprocessing import Pool

from radical.utils import which
from radical.pilot.states import *
from radical.pilot.utils.logger import logger

from radical.pilot.controller.input_file_transfer_worker import InputFileTransferWorker
from radical.pilot.controller.output_file_transfer_worker import OutputFileTransferWorker


# ----------------------------------------------------------------------------
#
class UnitManagerController(threading.Thread):
    """UnitManagerController handles backend interaction for the UnitManager 
    class. It is threaded and manages background worker processes. 
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, unit_manager_uid, scheduler, input_transfer_workers,
        output_transfer_workers, db_connection, db_connection_info):

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

        # The shard_data_manager handles data exchange between the worker
        # process and the API objects. The communication is unidirectional:
        # workers WRITE to _shared_data and API methods READ from _shared_data.
        # The strucuture of _shared_data is as follows:
        #
        # { unit1_uid: MongoDB document (dict),
        #   unit2_uid: MongoDB document (dict),
        #   ...
        # }
        #
        self._shared_data = dict()
        self._shared_data_lock = threading.Lock()

        # The manager-level list.
        #
        self._manager_callbacks = list()

        # The MongoDB database handle.
        self._db = db_connection

        if unit_manager_uid is None:
            # Try to register the UnitManager with the database.
            self._um_id = self._db.insert_unit_manager(
                scheduler=scheduler,
                input_transfer_workers=input_transfer_workers,
                output_transfer_workers=output_transfer_workers)
            self._num_input_transfer_workers = input_transfer_workers
            self._num_output_transfer_workers = output_transfer_workers
        else:
            um_json = self._db.get_unit_manager(unit_manager_id=unit_manager_id)
            self._um_id = unit_manager_uid
            self._num_input_transfer_workers = um_json["input_transfer_workers"]
            self._num_output_transfer_workers = um_json["output_transfer_workers"]

        # The INPUT transfer worker(s) are autonomous processes that
        # execute input file transfer requests concurrently.
        self._input_file_transfer_worker_pool = []
        for worker_number in range(1, self._num_input_transfer_workers+1):
            worker = InputFileTransferWorker(
                db_connection_info=db_connection_info, 
                unit_manager_id=self._um_id,
                number=worker_number
            )
            self._input_file_transfer_worker_pool.append(worker)
            worker.start()

        # The OUTPUT transfer worker(s) are autonomous processes that
        # execute output file transfer requests concurrently.
        self._output_file_transfer_worker_pool = []
        for worker_number in range(1, self._num_output_transfer_workers+1):
            worker = OutputFileTransferWorker(
                db_connection_info=db_connection_info, 
                unit_manager_id=self._um_id,
                number=worker_number
            )
            self._output_file_transfer_worker_pool.append(worker)
            worker.start()

    # ------------------------------------------------------------------------
    #
    @classmethod
    def uid_exists(cls, db_connection, unit_manager_uid):
        """Checks wether a particular unit manager UID exists.
        """
        exists = False

        if unit_manager_uid in db_connection.list_unit_manager_uids():
            exists = True

        return exists

    # ------------------------------------------------------------------------
    #
    @property
    def unit_manager_uid(self):
        """Returns the uid of the associated UnitManager
        """
        return self._um_id

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """stop() signals the process to finish up and terminate.
        """
        self._stop.set()
        self.join()
        logger.debug("Worker thread (ID: %s[%s]) for UnitManager %s stopped." %
                    (self.name, self.ident, self._um_id))

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_data(self, unit_uid):
        """Retruns the raw data (json dicts) of one or more ComputeUnits
           registered with this Worker / UnitManager
        """
        # Wait for the initialized event to assert proper operation.
        self._initialized.wait()

        return self._shared_data[unit_uid]["data"]

    # ------------------------------------------------------------------------
    #
    def call_callbacks(self, unit_id, new_state):
        """Wrapper function to call all all relevant callbacks, on unit-level
        as well as manager-level.
        """
        for cb in self._shared_data[unit_id]['callbacks']:
            try:
                cb(self._shared_data[unit_id]['facade_object'],
                   new_state)
            except Exception, ex:
                logger.error(
                    "Couldn't call callback function %s" % str(ex))

        # If we have any manager-level callbacks registered, we
        # call those as well!
        for cb in self._manager_callbacks:
            try:
                cb(self._shared_data[unit_id]['facade_object'],
                   new_state)
            except Exception, ex:
                logger.error(
                    "Couldn't call callback function %s" % str(ex))

    # ------------------------------------------------------------------------
    #
    # def _set_state(self, unit_uid, state, log):

    #     if not isinstance(log, list):
    #         log = [log]

    #     # Acquire the shared data lock.
    #     self._shared_data_lock.acquire()

    #     old_state = self._shared_data[unit_uid]["data"]["state"]

    #     # Update the database.
    #     self._db.set_compute_unit_state(unit_uid, state, log)

    #     # Update shared data.
    #     self._shared_data[unit_uid]["data"]["state"] = state
    #     self._shared_data[unit_uid]["data"]["statehistory"].append(state)
    #     self._shared_data[unit_uid]["data"]["log"].extend(log)

    #     # Call the callbacks
    #     if state != old_state:
    #         # On a state change, we fire zee callbacks.
    #         logger.info(
    #             "XX ComputeUnit '%s' state changed from '%s' to '%s'." %
    #             (unit_uid, old_state, state)
    #         )

    #         # The state of the unit has changed, We call all
    #         # unit-level callbacks to propagate this.
    #         self.call_callbacks(unit_uid, state)

    #     # Release the shared data lock.
    #     self._shared_data_lock.release()

    # ------------------------------------------------------------------------
    #
    def run(self):
        """run() is called when the process is started via
           PilotManagerController.start().
        """
        logger.debug("Worker thread (ID: %s[%s]) for UnitManager %s started." %
                    (self.name, self.ident, self._um_id))

        # transfer results contains the futures to the results of the
        # asynchronous transfer operations.
        transfer_results = list()

        while not self._stop.is_set():

            # =================================================================
            #
            # Check and update units. This needs to be optimized at
            # some point, i.e., state pulling should be conditional
            # or triggered by a tailable MongoDB cursor, etc.
            unit_list = self._db.get_compute_units(unit_manager_id=self._um_id)

            for unit in unit_list:
                unit_id = str(unit["_id"])

                new_state = unit["state"]
                if unit_id in self._shared_data:
                    old_state = self._shared_data[unit_id]["data"]["state"]
                else:
                    old_state = None
                    self._shared_data_lock.acquire()
                    self._shared_data[unit_id] = {
                        'data':          unit,
                        'callbacks':     [],
                        'facade_object': None
                    }
                    self._shared_data_lock.release()

                self._shared_data_lock.acquire()
                self._shared_data[unit_id]["data"] = unit
                self._shared_data_lock.release()

                if new_state != old_state:
                    # On a state change, we fire zee callbacks.
                    logger.info("RUN ComputeUnit '%s' state changed from '%s' to '%s'." % (unit_id, old_state, new_state))

                    # The state of the unit has changed, We call all
                    # unit-level callbacks to propagate this.
                    self.call_callbacks(unit_id, new_state)

            # After the first iteration, we are officially initialized!
            if not self._initialized.is_set():
                self._initialized.set()

            time.sleep(1)

        # shut down the autonomous input / output transfer worker(s)
        for worker in self._input_file_transfer_worker_pool:
            worker.terminate()
            worker.join()
            logger.debug("UnitManager.close(): %s terminated." % worker.name)

        for worker in self._output_file_transfer_worker_pool:
            worker.terminate()
            worker.join()
            logger.debug("UnitManager.close(): %s terminated." % worker.name)

    # ------------------------------------------------------------------------
    #
    def register_unit_callback(self, unit, callback_func):
        """Registers a callback function for a ComputeUnit.
        """
        unit_uid = unit.uid

        self._shared_data_lock.acquire()
        self._shared_data[unit_uid]['callbacks'].append(callback_func)
        self._shared_data_lock.release()

        # Add the facade object if missing, e.g., after a re-connect.
        if self._shared_data[unit_uid]['facade_object'] is None:
            self._shared_data_lock.acquire()
            self._shared_data[unit_uid]['facade_object'] = unit # weakref.ref(unit)
            self._shared_data_lock.release()

        # Callbacks can only be registered when the ComputeAlready has a
        # state. To partially address this shortcomming we call the callback
        # with the current ComputePilot state as soon as it is registered.
        self.call_callbacks(
            unit_uid,
            self._shared_data[unit_uid]["data"]["state"]
        )

    # ------------------------------------------------------------------------
    #
    def register_manager_callback(self, callback_func):
        """Registers a manager-level callback.
        """
        self._manager_callbacks.append(callback_func)

    # ------------------------------------------------------------------------
    #
    def get_unit_manager_data(self):
        """Returns the raw data (JSON dict) for a UnitManger.
        """
        return self._db.get_unit_manager(self._um_id)

    # ------------------------------------------------------------------------
    #
    def get_pilot_uids(self):
        """Returns the UIDs of the pilots registered with the UnitManager.
        """
        return self._db.unit_manager_list_pilots(self._um_id)

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_uids(self):
        """Returns the UIDs of all ComputeUnits registered with the
        UnitManager.
        """
        return self._db.unit_manager_list_compute_units(self._um_id)

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_states(self, unit_uids=None):
        """Returns the states of all ComputeUnits registered with the
        Unitmanager.
        """
        return self._db.get_compute_unit_states(
            self._um_id, unit_uids)

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_stdout(self, compute_unit_uid):
        """Returns the stdout for a compute unit.
        """
        return self._db.get_compute_unit_stdout(compute_unit_uid)

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_stderr(self, compute_unit_uid):
        """Returns the stderr for a compute unit.
        """
        return self._db.get_compute_unit_stderr(compute_unit_uid)

    # ------------------------------------------------------------------------
    #
    def add_pilots(self, pilots):
        """Links ComputePilots to the UnitManager.
        """
        # Extract the uids
        pids = []
        for pilot in pilots:
            pids.append(pilot.uid)

        self._db.unit_manager_add_pilots(unit_manager_id=self._um_id,
                                         pilot_ids=pids)

    # ------------------------------------------------------------------------
    #
    def remove_pilots(self, pilot_uids):
        """Unlinks one or more ComputePilots from the UnitManager.
        """
        self._db.unit_manager_remove_pilots(unit_manager_id=self._um_id,
                                            pilot_ids=pilot_uids)

    # ------------------------------------------------------------------------
    #
    def schedule_compute_units(self, pilot_uid, units, session):
        """Request the scheduling of one or more ComputeUnits on a
           ComputePilot.
        """

        # Get the credentials from the session.
        cred_dict = []
        for cred in session.credentials:
            cred_dict.append(cred.as_dict())

        unit_descriptions = list()
        wu_transfer = list()
        wu_notransfer = list()

        # Get some information about the pilot sandbox from the database.
        pilot_info = self._db.get_pilots(pilot_ids=pilot_uid)
        pilot_sandbox = pilot_info[0]['sandbox']

        # Split units into two different lists: the first list contains the CUs
        # that need file transfer and the second list contains the CUs that
        # don't. The latter is added to the pilot directly, while the former
        # is added to the transfer queue.
        for unit in units:
            if unit.description.input_data is None:
                wu_notransfer.append(unit.uid)
            else:
                wu_transfer.append(unit)

        # Add all units to the database.
        results = self._db.insert_compute_units(
            pilot_uid=pilot_uid,
            pilot_sandbox=pilot_sandbox,
            unit_manager_uid=self._um_id,
            units=units,
            unit_log=[]
        )

        assert len(units) == len(results)

        # Match results with units.
        for unit in units:
            # Create a shared data store entry
            self._shared_data[unit.uid] = {
                'data':          results[unit.uid],
                'callbacks':     [],
                'facade_object': unit # weakref.ref(unit)
            }

        # Bulk-add all non-transfer units-
        self._db.assign_compute_units_to_pilot(
            unit_uids=wu_notransfer,
            pilot_uid=pilot_uid
        )

        for unit in wu_notransfer:
            log = ["Scheduled for execution on ComputePilot %s." % pilot_uid]
            self._db.set_compute_unit_state(unit, PENDING_EXECUTION, log)
            #self._set_state(unit, PENDING_EXECUTION, log)

        logger.info(
            "Scheduled ComputeUnits %s for execution on ComputePilot '%s'." %
            (wu_notransfer, pilot_uid)
        )

        # Bulk-add all units that need transfer to the transfer queue.
        # Add the startup request to the request queue.
        if len(wu_transfer) > 0:
            for unit in wu_transfer:
                log = ["Scheduled for data tranfer to ComputePilot %s." % pilot_uid]
                self._db.set_compute_unit_state(unit.uid, PENDING_INPUT_TRANSFER, log)
                #self._set_state(unit.uid, PENDING_INPUT_TRANSFER, log)
