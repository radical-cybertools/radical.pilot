"""
.. module:: radical.pilot.mpworker.output_file_transfer_worker
.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import time
import multiprocessing

from bson.objectid import ObjectId
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

        # Try to connect to the database and create a tailable cursor.
        try: 
            connection = self.db_connection_info.get_db_handle()
            db = connection[self.db_connection_info.dbname]
            um_col = db["%s.w" % self.db_connection_info.session_id] 
            logger.debug("Connected to database %s." % db.host)
        except Exception, ex: 
            logger.error("Connection error: %s" % str(ex))
            return

        while True:

            time.sleep(1)

            transfer = None

            # See if we can find a new transfer request.
            transfer = um_col.find_and_modify(
                query={"unitmanager": self.unit_manager_id, 
                       "state": "PendingOutputTransfer"},
                update={"$set": {"state": "TransferringOutput"}},
                limit=BULK_LIMIT
            )

            if transfer is not None:
                logger.info("Transferring: %s" % transfer["description"]["output_data"])
                time.sleep(3)

                # Update the CU's state to 'DONE' if transfer was successfull, 
                # to 'FAILED' otherwise. 
                um_col.update(
                    {"_id": transfer["_id"]},
                    {"$set": {"state": "Done"}}
                )
