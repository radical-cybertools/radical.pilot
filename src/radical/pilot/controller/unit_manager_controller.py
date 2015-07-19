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
import datetime
import threading

from multiprocessing import Pool

from radical.utils        import which
from radical.utils        import Url
from radical.pilot.types  import *
from radical.pilot.states import *
from radical.pilot.utils.logger import logger

from radical.pilot.controller.input_file_transfer_worker import InputFileTransferWorker
from radical.pilot.controller.output_file_transfer_worker import OutputFileTransferWorker

from radical.pilot.staging_directives import TRANSFER, LINK, COPY, MOVE

IDLE_TIME = 1.0  # seconds to sleep between activities

# ----------------------------------------------------------------------------
#
class UnitManagerController(threading.Thread):
    """UnitManagerController handles backend interaction for the UnitManager 
    class. It is threaded and manages background worker processes. 
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, unit_manager_uid, session, db_connection, db_connection_info,
        scheduler=None, input_transfer_workers=None,
        output_transfer_workers=None):

        self._session = session

        # Multithreading stuff
        threading.Thread.__init__(self)

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
        self._manager_callbacks = dict()

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
            um_json = self._db.get_unit_manager(unit_manager_id=unit_manager_uid)
            self._um_id = unit_manager_uid
            self._num_input_transfer_workers = um_json["input_transfer_workers"]
            self._num_output_transfer_workers = um_json["output_transfer_workers"]

        # The INPUT transfer worker(s) are autonomous processes that
        # execute input file transfer requests concurrently.
        self._input_file_transfer_worker_pool = []
        for worker_number in range(1, self._num_input_transfer_workers+1):
            worker = InputFileTransferWorker(
                session=self._session,
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
                session=self._session,
                db_connection_info=db_connection_info, 
                unit_manager_id=self._um_id,
                number=worker_number
            )
            self._output_file_transfer_worker_pool.append(worker)
            worker.start()

        self._callback_histories = dict ()

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
        logger.debug("uworker  %s stopping" % (self.name))
        self._stop.set()
        self.join()
        logger.debug("uworker  %s stopped" % (self.name))
      # logger.debug("Worker thread (ID: %s[%s]) for UnitManager %s stopped." %
      #             (self.name, self.ident, self._um_id))

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_data(self, unit_uid):
        """Returns the raw data (json dicts) of one or more ComputeUnits
           registered with this Worker / UnitManager
        """
        # Wait for the initialized event to assert proper operation.
        self._initialized.wait()

        return self._shared_data[unit_uid]["data"]

    # ------------------------------------------------------------------------
    #
    def call_unit_state_callbacks(self, unit_id, new_state):
        """Wrapper function to call all all relevant callbacks, on unit-level
        as well as manager-level.
        """

        # this is the point where, at the earliest, the application could have
        # been notified about unit state changes.  So we record that event.
        if  not unit_id in self._callback_histories :
            self._callback_histories[unit_id] = list()
        self._callback_histories[unit_id].append (
                {'timestamp' : datetime.datetime.utcnow(), 
                 'state'     : new_state})

        for [cb, cb_data] in self._shared_data[unit_id]['callbacks']:
            try:

                if self._shared_data[unit_id]['facade_object'] :
                    if  cb_data :
                        cb(self._shared_data[unit_id]['facade_object'], new_state, cb_data)
                    else :
                        cb(self._shared_data[unit_id]['facade_object'], new_state)
                else :
                    logger.error("Couldn't call callback (no pilot instance)")
            except Exception as e:
                logger.exception(
                    "Couldn't call callback function %s" % e)
                raise

        # If we have any manager-level callbacks registered, we
        # call those as well!
        if  not UNIT_STATE in self._manager_callbacks :
            self._manager_callbacks[UNIT_STATE] = list()

        for [cb, cb_data] in self._manager_callbacks[UNIT_STATE]:
            if not self._shared_data[unit_id]['facade_object'] :
                logger.warning ('skip cb for incomple unit (%s: %s)' % (unit_id, new_state))
                break

            try:
                if  cb_data :
                    cb(self._shared_data[unit_id]['facade_object'], new_state, cb_data)
                else :
                    cb(self._shared_data[unit_id]['facade_object'], new_state)
            except Exception as e:
                logger.exception(
                    "Couldn't call callback function %s" % e)
                raise

        # If we meet a final state, we record the object's callback history for
        # later evaluation.
        if  new_state in (DONE, FAILED, CANCELED) :
            self._db.publish_compute_unit_callback_history (unit_id, self._callback_histories[unit_id])


    # ------------------------------------------------------------------------
    #
    def run(self):
        """run() is called when the process is started via
           PilotManagerController.start().
        """

        # make sure to catch sys.exit (which raises SystemExit)
        try :

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
                action    = False

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
                        self.call_unit_state_callbacks(unit_id, new_state)

                        action = True

                # After the first iteration, we are officially initialized!
                if not self._initialized.is_set():
                    self._initialized.set()

                # sleep a little if this cycle was idle
                if  not action :
                    time.sleep(IDLE_TIME)


        except SystemExit as e :
            logger.exception ("unit manager controller thread caught system exit -- forcing application shutdown")
            import thread
            thread.interrupt_main ()


        finally :
            # shut down the autonomous input / output transfer worker(s)
            for worker in self._input_file_transfer_worker_pool:
                logger.debug("uworker %s stops   itransfer %s" % (self.name, worker.name))
                worker.stop ()
                logger.debug("uworker %s stopped itransfer %s" % (self.name, worker.name))

            for worker in self._output_file_transfer_worker_pool:
                logger.debug("uworker %s stops   otransfer %s" % (self.name, worker.name))
                worker.stop ()
                logger.debug("uworker %s stopped otransfer %s" % (self.name, worker.name))


    # ------------------------------------------------------------------------
    #
    def register_unit_callback(self, unit, callback_func, callback_data=None):
        """Registers a callback function for a ComputeUnit.
        """
        unit_uid = unit.uid

        self._shared_data_lock.acquire()
        self._shared_data[unit_uid]['callbacks'].append([callback_func, callback_data])
        self._shared_data_lock.release()

        # Add the facade object if missing, e.g., after a re-connect.
        if self._shared_data[unit_uid]['facade_object'] is None:
            self._shared_data_lock.acquire()
            self._shared_data[unit_uid]['facade_object'] = unit # weakref.ref(unit)
            self._shared_data_lock.release()

        # Callbacks can only be registered when the ComputeUnit lready has a
        # state. To partially address this shortcomming we call the callback
        # with the current ComputeUnit state as soon as it is registered.
        self.call_unit_state_callbacks(
            unit_uid,
            self._shared_data[unit_uid]["data"]["state"]
        )

    # ------------------------------------------------------------------------
    #
    def register_manager_callback(self, callback_func, metric, callback_data=None):
        """Registers a manager-level callback.
        """
        if not metric in self._manager_callbacks :
            self._manager_callbacks[metric] = list()

        self._manager_callbacks[metric].append([callback_func, callback_data])


    # ------------------------------------------------------------------------
    #
    def fire_manager_callback(self, metric, obj, value):
        """Fire a manager-level callback.
        """
        if  not metric in self._manager_callbacks :
            self._manager_callbacks[metric] = list()

        for [cb, cb_data] in self._manager_callbacks[metric] :
            try:
                if  cb_data :
                    cb (obj, value, cb_data)
                else :
                    cb (obj, value)
            except Exception as e:
                logger.exception ("Couldn't call '%s' callback function %s: %s" \
                           % (metric, cb, e))
                raise

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
    def publish_compute_units(self, units):
        """register the unscheduled units in the database"""

        # Add all units to the database.
        results = self._db.insert_compute_units(
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


    # ------------------------------------------------------------------------
    #
    def schedule_compute_units(self, pilot_uid, units):
        """Request the scheduling of one or more ComputeUnits on a
           ComputePilot.
        """

        try:
            cu_transfer   = list()
            cu_notransfer = list()

            # Get some information about the pilot sandbox from the database.
            pilot_info = self._db.get_pilots(pilot_ids=pilot_uid)
            # TODO: this hack below relies on what?! That there is just one pilot?
            pilot_sandbox = pilot_info[0]['sandbox']

            # Split units into two different lists: the first list contains the CUs
            # that need file transfer and the second list contains the CUs that
            # don't. The latter is added to the pilot directly, while the former
            # is added to the transfer queue.
            for unit in units:

                # Create object for staging status tracking
                unit.FTW_Input_Status = None
                unit.FTW_Input_Directives = []
                unit.Agent_Input_Status = None
                unit.Agent_Input_Directives = []
                unit.FTW_Output_Status = None
                unit.FTW_Output_Directives = []
                unit.Agent_Output_Status = None
                unit.Agent_Output_Directives = []

                # Split the input staging directives over the transfer worker and the agent
                input_sds = unit.description.input_staging
                if not isinstance(input_sds, list):
                    # Ugly, but is a workaround for iterating on attribute interface
                    # TODO: Verify if this piece of code is actually still required
                    if input_sds:
                        input_sds = [input_sds]
                    else:
                        input_sds = []

                for input_sd_entry in input_sds:
                    action = input_sd_entry['action']
                    source = Url(input_sd_entry['source'])
                    target = Url(input_sd_entry['target'])

                    new_sd = {'action':   action,
                              'source':   str(source),
                              'target':   str(target),
                              'flags':    input_sd_entry['flags'],
                              'priority': input_sd_entry['priority'],
                              'state':    PENDING
                    }

                    if action in [LINK, COPY, MOVE]:
                        unit.Agent_Input_Directives.append(new_sd)
                        unit.Agent_Input_Status = PENDING
                    elif action in [TRANSFER]:
                        if source.scheme and source.scheme != 'file':
                            # If there is a scheme and it is different than "file",
                            # assume a remote pull from the agent
                            unit.Agent_Input_Directives.append(new_sd)
                            unit.Agent_Input_Status = PENDING
                        else:
                            # Transfer from local to sandbox
                            unit.FTW_Input_Directives.append(new_sd)
                            unit.FTW_Input_Status = PENDING
                    else:
                        logger.warn('Not sure if action %s makes sense for input staging' % action)

                # Split the output staging directives over the transfer worker and the agent
                output_sds = unit.description.output_staging
                if not isinstance(output_sds, list):
                    # Ugly, but is a workaround for iterating on att iface
                    # TODO: Verify if this piece of code is actually still required
                    if output_sds:
                        output_sds = [output_sds]
                    else:
                        output_sds = []

                for output_sds_entry in output_sds:
                    action = output_sds_entry['action']
                    source = Url(output_sds_entry['source'])
                    target = Url(output_sds_entry['target'])

                    new_sd = {'action':   action,
                              'source':   str(source),
                              'target':   str(target),
                              'flags':    output_sds_entry['flags'],
                              'priority': output_sds_entry['priority'],
                              'state':    PENDING
                    }

                    if action == LINK or action == COPY or action == MOVE:
                        unit.Agent_Output_Directives.append(new_sd)
                        unit.Agent_Output_Status = NEW
                    elif action == TRANSFER:
                        if target.scheme and target.scheme != 'file':
                            # If there is a scheme and it is different than "file",
                            # assume a remote push from the agent
                            unit.Agent_Output_Directives.append(new_sd)
                            unit.Agent_Output_Status = NEW
                        else:
                            # Transfer from sandbox back to local
                            unit.FTW_Output_Directives.append(new_sd)
                            unit.FTW_Output_Status = NEW
                    else:
                        logger.warn('Not sure if action %s makes sense for output staging' % action)

            # Bulk-add all units
            self._db.assign_compute_units_to_pilot(
                units=units,
                pilot_uid=pilot_uid,
                pilot_sandbox=pilot_sandbox
            )

            for unit in units:
                # DON'T set state before pilot is assigned -- otherwise units
                # are picked up by the FTW
                log = "Scheduled for data transfer to ComputePilot %s." % pilot_uid
                self._db.set_compute_unit_state(unit.uid, PENDING_INPUT_STAGING, log)


            logger.info(
                "Scheduled ComputeUnits %s on ComputePilot '%s'." %
                (units, pilot_uid)
            )

        except Exception, e:
            logger.exception ('error in unit manager controller (schedule())')
            raise

    # ------------------------------------------------------------------------
    #
    def unschedule_compute_units(self, units):
        """
        set the unit state to UNSCHEDULED
        """

        try:
            unit_ids = [unit.uid for unit in units]
            self._db.set_compute_unit_state(unit_ids, UNSCHEDULED, "unit remains unscheduled")

        except Exception, e:
            logger.exception ('error in unit manager controller (unschedule())')
            raise

