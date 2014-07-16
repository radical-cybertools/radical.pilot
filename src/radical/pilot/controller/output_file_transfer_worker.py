"""
.. module:: radical.pilot.controller.output_file_transfer_worker
.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import datetime
import traceback
import multiprocessing

from bson.objectid import ObjectId
from radical.pilot.states import * 
from radical.pilot.utils.logger import logger

# BULK_LIMIT defines the max. number of transfer requests to pull from DB.
BULK_LIMIT=1

# ----------------------------------------------------------------------------
#
class OutputFileTransferWorker(multiprocessing.Process):
    """OutputFileTransferWorker handles the staging of the output files
    for a UnitManagerController.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, session, db_connection_info, unit_manager_id, number=None):

        self._session = session

        # Multiprocessing stuff
        multiprocessing.Process.__init__(self)
        self.daemon = True

        self.db_connection_info = db_connection_info
        self.unit_manager_id = unit_manager_id

        self.name = "OutputFileTransferWorker-%s" % str(number)

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the process when Process.start() is called.
        """

        # Try to connect to the database and create a tailable cursor.
        try:
            connection = self.db_connection_info.get_db_handle()
            db = connection[self.db_connection_info.dbname]
            um_col = db["%s.w" % self.db_connection_info.session_id]
            logger.debug("Connected to MongoDB. Serving requests for UnitManager %s." % self.unit_manager_id)

        except Exception, ex:
            logger.error("Connection error: %s. %s" % (str(ex), traceback.format_exc()))
            return

        while True:
            compute_unit = None

            # See if we can find a ComputeUnit that is waiting for
            # output file transfer.
            ts = datetime.datetime.utcnow()
            compute_unit = um_col.find_and_modify(
                query={"unitmanager": self.unit_manager_id,
                       "state" : PENDING_OUTPUT_TRANSFER},
                update={"$set" : {"state": TRANSFERRING_OUTPUT},
                        "$push": {"statehistory": {"state": TRANSFERRING_OUTPUT, "timestamp": ts}}},
                limit=BULK_LIMIT
            )
            state = TRANSFERRING_OUTPUT

            if compute_unit is None:
                # Sleep a bit if no new units are available.
                time.sleep(1)
            else:
                # AM: The code below seems wrong when BULK_LIMIT != 1 -- the
                # compute_unit will be a list then I assume.
                try:
                    log_messages = []

                    # We have found a new CU. Now we can process the transfer
                    # directive(s) wit SAGA.
                    compute_unit_id = str(compute_unit["_id"])
                    remote_sandbox = compute_unit["sandbox"]
                    transfer_directives = compute_unit["description"]["output_data"]

                    logger.info("Processing output file transfers for ComputeUnit %s" % compute_unit_id)
                    # Loop over all transfer directives and execute them.
                    for td in transfer_directives:

                        state_doc = um_col.find_one(
                            {"_id": ObjectId(compute_unit_id)},
                            fields=["state"]
                        )
                        if state_doc['state'] == CANCELED:
                            logger.info("Compute Unit Canceled, interrupting output file transfers.")
                            state = CANCELED
                            break

                        st = td.split(">")
                        abs_source = "%s/%s" % (remote_sandbox, st[0].strip())

                        if len(st) == 1:
                            abs_target = "file://localhost/%s" % os.getcwd()
                        elif len(st) == 2:
                            abs_target = "file://localhost/%s" % os.path.abspath(st[1].strip())
                        else:
                            raise Exception("Invalid transfer directive: %s" % td)

                        log_msg = "Transferring output file %s -> %s" % (abs_source, abs_target)
                        log_messages.append(log_msg)
                        logger.debug(log_msg)

                        output_file = saga.filesystem.File(saga.Url(abs_source),
                            session=self._session
                        )
                        output_file.copy(saga.Url(abs_target))
                        output_file.close()

                        # Update the CU's state to 'DONE' if (all) transfers were successful.
                        state = DONE

                    ts = datetime.datetime.utcnow()
                    um_col.update(
                        {"_id": ObjectId(compute_unit_id)},
                        {"$set": {"state": state},
                         "$push": {"statehistory": {"state": state, "timestamp": ts}},
                         "$pushAll": {"log": log_messages}}                    
                    )

                except Exception, ex:
                    # Update the CU's state to 'FAILED'.
                    ts = datetime.datetime.utcnow()
                    log_messages = "Output transfer failed: %s\n%s" % (str(ex), traceback.format_exc())
                    um_col.update(
                        {"_id": ObjectId(compute_unit_id)},
                        {"$set": {"state": FAILED},
                         "$push": {"statehistory": {"state": FAILED, "timestamp": ts}},
                         "$push": {"log": log_messages}}
                    )
                    logger.error(log_messages)

