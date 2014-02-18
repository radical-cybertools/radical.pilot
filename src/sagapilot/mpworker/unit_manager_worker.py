#pylint: disable=C0301, C0103, W0212

"""
.. module:: sinon.mpworker.unit_manager_worker
   :platform: Unix
   :synopsis: Implements a multiprocessing worker backend for
              the UnitManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"


import time
import multiprocessing
from Queue import Empty

import sagapilot.states as state

from radical.utils import which
from sagapilot.utils.logger import logger


# ----------------------------------------------------------------------------
#
class UnitManagerWorker(multiprocessing.Process):
    """UnitManagerWorker is a multiprocessing worker that handles backend
       interaction for the UnitManager class.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, unit_manager_uid, unit_manager_data, db_connection):

        # Multiprocessing stuff
        multiprocessing.Process.__init__(self)
        self.daemon = True

        # Stop event can be set to terminate the main loop
        self._stop = multiprocessing.Event()
        self._stop.clear()

        # Initialized is set, once the run loop has pulled status
        # at least once. Other functions use it as a guard.
        self._initialized = multiprocessing.Event()
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
        shard_data_manager = multiprocessing.Manager()
        self._shared_data = shard_data_manager.dict()

        # The callback dictionary. The structure is as follows:
        #
        # { unit1_uid : [func_ptr, func_ptr, func_ptr, ...],
        #   unit2_uid : [func_ptr, func_ptr, func_ptr, ...],
        #   ...
        # }
        #
        self._callbacks = shard_data_manager.dict()

        # The MongoDB database handle.
        self._db = db_connection

        self._um_id = unit_manager_uid

        # The different command queues hold pending operations
        # that are passed to the worker. Queues are inspected during
        # runtime in the run() loop and the worker acts upon them accordingly.
        self._transfer_requests = multiprocessing.Queue()

        if unit_manager_uid is None:
            # Try to register the PilotManager with the database.
            self._um_id = self._db.insert_unit_manager(
                unit_manager_data=unit_manager_data)
        else:
            self._um_id = unit_manager_uid

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
        logger.debug("Worker process (PID: %s) for UnitManager %s stopped." %
                    (self.pid, self._um_id))

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_data(self, unit_uid):
        """Retruns the raw data (json dicts) of one or more ComputeUnits
           registered with this Worker / UnitManager
        """
        # Wait for the initialized event to assert proper operation.
        self._initialized.wait()

        return self._shared_data[unit_uid]

    # ------------------------------------------------------------------------
    #
    def run(self):
        """run() is called when the process is started via
           PilotManagerWorker.start().
        """
        logger.debug("Worker process for UnitManager %s started with PID %s."
                    % (self._um_id, self.pid))

        while not self._stop.is_set():

            # =================================================================
            #
            # Process any new transfer requests.

            # Check if there are any pilots to cancel.
            try:
                request = self._transfer_requests.get_nowait()
                self._db.set_compute_unit_state(request["unit_uid"], state.TRANSFERRING_INPUT, ["start transferring"])
                logger.warning("about to transfer unit %s", request)

                self._db.assign_compute_units_to_pilot(
                    unit_uids=request["unit_uid"],
                    pilot_uid=request["pilot_uid"]
                )

                self._db.set_compute_unit_state(request["unit_uid"], state.PENDING_EXECUTION, ["end transferring"])

                #logger.info("Scheduled ComputeUnits %s for execution on ComputePilot '%s'." % (wu_notransfer.keys(), pilot_uid))
            except Empty:
                pass

            # =================================================================
            #
            # Check and update units. This needs to be optimized at
            # some point, i.e., state pulling should be conditional
            # or triggered by a tailable MongoDB cursor, etc.
            unit_list = self._db.get_compute_units(unit_manager_id=self._um_id)

            for unit in unit_list:
                unit_id = str(unit["_id"])

                new_state = unit["info"]["state"]
                if unit_id in self._shared_data:
                    old_state = self._shared_data[unit_id]["info"]["state"]
                else:
                    old_state = None

                if new_state != old_state:
                    # On a state change, we fire zee callbacks.
                    logger.info("ComputeUnit '%s' state changed from '%s' to '%s'." % (unit_id, old_state, new_state))
                    if unit_id in self._callbacks:
                        for cb in self._callbacks[unit_id]:
                            cb(unit_id, new_state)

                self._shared_data[unit_id] = unit

            # After the first iteration, we are officially initialized!
            if not self._initialized.is_set():
                self._initialized.set()
                logger.debug("Worker status set to 'initialized'.")

            time.sleep(1)

    # ------------------------------------------------------------------------
    #
    def register_unit_state_callback(self, unit_uid, callback_func):
        """Registers a callback function.
        """
        if unit_uid not in self._callbacks:
            # First callback ever registered for pilot_uid.
            self._callbacks[unit_uid] = [callback_func]
        else:
            # Additional callback for unit_uid.
            self._callbacks[unit_uid].append(callback_func)

        # Callbacks can only be registered when the ComputeAlready has a
        # state. To address this shortcomming we call the callback with the
        # current ComputePilot state as soon as it is registered.
        print self._shared_data[unit_uid]
        callback_func(unit_uid, self._shared_data[unit_uid]["info"]["state"])

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
        """Returns the UIDs of all ComputeUnits registered with the UnitManager.
        """
        return self._db.unit_manager_list_compute_units(self._um_id)

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_states(self, unit_uids=None):
        """Returns the states of all ComputeUnits registered with the Unitmanager.
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
    def schedule_compute_units(self, pilot_uid, unit_descriptions):
        """Request the scheduling of one or more ComputeUnits on a
           ComputePilot.
        """
        wu_uids = self._db.insert_compute_units(
            pilot_uid=pilot_uid,
            unit_manager_uid=self._um_id,
            unit_descriptions=unit_descriptions,
            unit_log=[]
        )

        # At this point we have added all compute units to database. Now we
        # sort them in two different lists: the first list contains the CUs
        # that need file transfer and the second list contains the CUs that
        # don't. The latter is added to the pilot directly, while the former
        # is added to the transfer queue.

        assert len(wu_uids) == len(unit_descriptions)

        wu_transfer = dict()
        wu_notransfer = dict()

        for idx in range(0, len(unit_descriptions)):
            if unit_descriptions[idx].input_data is None:
                wu_notransfer[wu_uids[idx]] = unit_descriptions[idx]
            else:
                wu_transfer[wu_uids[idx]] = unit_descriptions[idx]

        # Bulk-add all non-transfer units
        self._db.assign_compute_units_to_pilot(
            unit_uids=wu_notransfer.keys(),
            pilot_uid=pilot_uid
        )

        logger.info("Scheduled ComputeUnits %s for execution on ComputePilot '%s'." % (wu_notransfer.keys(), pilot_uid))

        # Bulk-add all units that need transfer to the transfer queue.
        # Add the startup request to the request queue.
        for unit_id in wu_transfer:
            self._transfer_requests.put(
                {"pilot_uid": pilot_uid,
                 "unit_uid": str(unit_id)}
            )

        logger.info("Data transfer scheduled for ComputeUnits %s to ComputePilot '%s'." % (wu_transfer.keys(), pilot_uid))

        # Return UIDs as strings.
        return [str(uid) for uid in wu_uids]
