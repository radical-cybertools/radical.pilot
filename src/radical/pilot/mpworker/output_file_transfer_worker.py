"""
.. module:: radical.pilot.mpworker.output_file_transfer_worker
.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import traceback
import multiprocessing

from bson.objectid import ObjectId
from radical.pilot.utils.logger import logger
from radical.pilot.credentials import SSHCredential

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
    def __init__(self, db_connection_info, unit_manager_id):

        # Multiprocessing stuff
        multiprocessing.Process.__init__(self)
        self.daemon = True

        self.db_connection_info = db_connection_info
        self.unit_manager_id = unit_manager_id

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the process when Process.start() is called.
        """

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
            time.sleep(1)

            compute_unit = None

            # See if we can find a ComputeUnit that is waiting for
            # output file transfer.
            compute_unit = um_col.find_and_modify(
                query={"unitmanager": self.unit_manager_id,
                       "state": "PendingOutputTransfer"},
                update={"$set": {"state": "TransferringOutput"}},
                limit=BULK_LIMIT
            )

            if compute_unit is not None:

                try:
                    # We have found one. Now we can process the transfer
                    # directive(s) wit SAGA.
                    compute_unit_id = str(compute_unit["_id"])
                    remote_sandbox = compute_unit["sandbox"]
                    transfer_directives = compute_unit["description"]["output_data"]

                    abs_directives = []

                    for td in transfer_directives:
                        source = td.split(">")
                        abs_source = "%s/%s" % (remote_sandbox, source[0].strip())
                        if len(td) > 1:
                            abs_target = "file://localhost/%s" % os.path.abspath(source[1].strip())
                        else:
                            abs_target = "file://localhost/%s" % os.getcwd()

                        abs_directives.append({"source": abs_source, "target" : abs_target})

                    logger.info("Processing output data transfer for ComputeUnit %s: %s" \
                        % (compute_unit_id, abs_directives))

                    log_messages = []
                    for atd in abs_directives:

                        output_file = saga.filesystem.File(saga.Url(atd["source"]),
                            session=saga_session)
                        output_file.copy(saga.Url(atd["target"]))
                        output_file.close()

                        log_messages.append("Successfully transferred output file %s -> %s" \
                            % (atd["source"], atd["target"]))

                    # Update the CU's state to 'DONE' if all transfers were successfull.
                    um_col.update(
                        {"_id": ObjectId(compute_unit_id)},
                        {"$set": {"state": "Done"},
                         "$pushAll": {"log": log_messages}}                    
                    )

                except Exception, ex:
                    # Update the CU's state 'FAILED'.
                    log_messages = "Output transfer failed: %s" % str(ex)
                    um_col.update(
                        {"_id": ObjectId(compute_unit_id)},
                        {"$set": {"state": "Failed"},
                         "$push": {"log": log_messages}}
                    )
