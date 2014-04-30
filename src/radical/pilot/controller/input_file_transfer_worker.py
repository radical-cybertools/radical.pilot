"""
.. module:: radical.pilot.controller.input_file_transfer_worker
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
from radical.pilot.credentials import SSHCredential
from radical.pilot.staging_directives import TRANSFER

# BULK_LIMIT defines the max. number of transfer requests to pull from DB.
BULK_LIMIT=1

# ----------------------------------------------------------------------------
#
class InputFileTransferWorker(multiprocessing.Process):
    """InputFileTransferWorker handles the staging of input files
    for a UnitManagerController.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, db_connection_info, unit_manager_id, number=None):

        # Multiprocessing stuff
        multiprocessing.Process.__init__(self)
        self.daemon = True

        self.db_connection_info = db_connection_info
        self.unit_manager_id = unit_manager_id

        self.name = "InputFileTransferWorker-%s" % str(number)

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the process when Process.start() is called.
        """

        logger.info("Starting InputFileTransferWorker")
        #return


        # saga_session holds the SSH context infos.
        saga_session = saga.Session()

        # Try to connect to the database and create a tailable cursor.
        try:
            connection = self.db_connection_info.get_db_handle()
            db = connection[self.db_connection_info.dbname]
            um_col = db["%s.w" % self.db_connection_info.session_id]
            logger.debug("Connected to database %s." % db.host)

            session_col = db["%s" % self.db_connection_info.session_id]
            session = session_col.find(
                {"_id": ObjectId(self.db_connection_info.session_id)},
                {"credentials": 1}
            )

            for cred_dict in session[0]["credentials"]:
                cred = SSHCredential.from_dict(cred_dict)
                saga_session.add_context(cred._context)
                logger.debug("Added SSH context info: %s." % cred._context)

        except Exception, ex:
            tb = traceback.format_exc()
            logger.error("Connection error: %s. %s" % (str(ex), tb))
            return

        while True:
            # See if we can find a ComputeUnit that is waiting for
            # input file transfer.
            compute_unit = None

            ts = datetime.datetime.utcnow()
            compute_unit = um_col.find_and_modify(
                query={"unitmanager": self.unit_manager_id,
                       "state" : PENDING_INPUT_STAGING},
                update={"$set" : {"state": STAGING_INPUT},
                        "$push": {"statehistory": {"state": STAGING_INPUT, "timestamp": ts}}},
                limit=BULK_LIMIT
            )

            if compute_unit is None:
                # Sleep a bit if no new units are available.
                time.sleep(1)
            else:
                try:
                    log_messages = []

                    # We have found a new CU. Now we can process the transfer
                    # directive(s) wit SAGA.
                    compute_unit_id = str(compute_unit["_id"])
                    remote_sandbox = compute_unit["sandbox"]
                    input_staging = compute_unit["description"]["input_staging"]

                    # We need to create the WU's directory in case it doesn't exist yet.
                    log_msg = "Creating ComputeUnit sandbox directory %s." % remote_sandbox
                    log_messages.append(log_msg)
                    logger.info(log_msg)

                    # Creating the sandbox directory.
                    try:
                        wu_dir = saga.filesystem.Directory(
                            remote_sandbox,
                            flags=saga.filesystem.CREATE_PARENTS,
                            session=saga_session)
                        wu_dir.close()
                    except Exception, ex:
                        tb = traceback.format_exc()
                        logger.info('Error: %s. %s' % (str(ex), tb))


                    logger.info("Processing input file transfers for ComputeUnit %s" % compute_unit_id)
                    # Loop over all transfer directives and execute them.
                    for sd in input_staging:

                        # Only deal with TRANSFER, all the other actions are taken care of by the agent.
                        if sd['action'] != TRANSFER:
                            logger.info("Action != Transfer, skipping ...")
                            continue
                        logger.info("Action == Transfer, let the games begin!")

                        abs_src = os.path.abspath(sd['source'])
                        input_file_url = saga.Url("file://localhost/%s" % abs_src)
                        if not sd['target']:
                            target = remote_sandbox
                        else:
                            target = "%s/%s" % (remote_sandbox, sd['target'])

                        log_msg = "Transferring input file %s -> %s" % (input_file_url, target)
                        log_messages.append(log_msg)
                        logger.debug(log_msg)

                        # Execute the transfer.
                        input_file = saga.filesystem.File(
                            input_file_url,
                            session=saga_session
                        )
                        logger.info("File object instantiated")
                        try:
                            input_file.copy(target)
                        except Exception, ex:
                            tb = traceback.format_exc()
                            logger.info('Error: %s. %s' % (str(ex), tb))

                        logger.info("File copied")
                        input_file.close()
                        logger.info("File closed")

                    # Update the CU's state to 'PENDING_EXECUTION' if all 
                    # transfers were successful.
                    ts = datetime.datetime.utcnow()
                    um_col.update(
                        {"_id": ObjectId(compute_unit_id)},
                        {"$set": {"state": PENDING_EXECUTION},
                         "$push": {"statehistory": {"state": PENDING_EXECUTION, "timestamp": ts}},
                         "$pushAll": {"log": log_messages}}                    
                    )

                except Exception, ex:
                    # Update the CU's state 'FAILED'.
                    ts = datetime.datetime.utcnow()
                    log_messages = "Input transfer failed: %s" % str(ex)
                    logger.debug(log_msg)
                    # um_col.update(
                    #     {"_id": ObjectId(compute_unit_id)},
                    #     {"$set": {"state": FAILED},
                    #      "$push": {"statehistory": {"state": FAILED, "timestamp": ts}},
                    #      "$push": {"log": log_messages}}
                    # )
