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


import os
import time
import saga
import datetime
import multiprocessing
from Queue import Empty

import sagapilot.states as state

from bson.objectid import ObjectId
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

        self._stop = multiprocessing.Event()
        self._stop.clear()

        self._um_id = unit_manager_uid
        self._db = db_connection

        # The different command queues hold pending operations
        # that are passed to the worker. Command queues are inspected during
        # runtime in the run() loop and the worker acts upon them accordingly.

        #self._cancel_pilot_requests  = multiprocessing.Queue()
        self._schedule_compute_unit_requests = multiprocessing.Queue()

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
        logger.info("Worker process (PID: %s) for UnitManager %s stopped." %
            (self.pid, self._um_id))

    # ------------------------------------------------------------------------
    #
    def run(self):
        """run() is called when the process is started via
           PilotManagerWorker.start().
        """
        logger.info("Worker process for UnitManager %s started with PID %s."
                    % (self._um_id, self.pid))

        while not self._stop.is_set():

            # Check if there are any compute_units to start.
            # try:
            #     request = self._schedule_compute_unit_requests.get_nowait()
            #     self._execute_schedule_compute_unit(request)
            # except Empty:
            #     pass

            # Check and update pilots:
            #   * list all pilots and check their state
            #result = self._db.get_pilots(self._pm_id)

            time.sleep(1)


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
        """Returns the UIDs of all WorkUnits registered with the UnitManager.
        """
        return self._db.unit_manager_list_work_units(self._um_id)

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_data(self, compute_unit_uid):
        """Returns the UIDs of all WorkUnits registered with the UnitManager.
        """
        return self._db.get_workunits(
            workunit_manager_id=self._um_id, 
            workunit_ids=[compute_unit_uid]
        )

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_states(self, work_unit_uids=None):
        """Returns the states of all WorkUnits registered with the Unitmanager.
        """
        return self._db.get_workunit_states(self._um_id, workunit_ids=work_unit_uids)

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_stdout(self, work_unit_uid):
        """Returns the stdout for a compute unit.
        """
        return self._db.get_workunit_stdout(work_unit_uid)

    # ------------------------------------------------------------------------
    #
    def get_compute_unit_stderr(self, work_unit_uid):
        """Returns the stderr for a compute unit.
        """
        return self._db.get_workunit_stderr(work_unit_uid)

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
    # def _execute_schedule_compute_unit(self, request):
    #     """document me"""
    #     logger.info("Scheduling ComputeUnit '%s' to ComputePilot '%s'."
    #                 % (request['compute_unit_uid'], request['pilot_uid']))

    #     self._db.assign_compute_unit_to_pilot(
    #         compute_unit_uid=request['compute_unit_uid'],
    #         pilot_uid=request['pilot_uid']
    #     )

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

        self._db.assign_compute_units_to_pilot(
            unit_uids=wu_uids,
            pilot_uid=pilot_uid
        )

        # Return UIDs as strings.
        return [str(uid) for uid in wu_uids]
