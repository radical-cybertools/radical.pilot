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
            um_col = db["%s.wm" % self.db_connection_info.session_id] 
            logger.debug("Connected to database %s." % db.host)
        except Exception, ex: 
            logger.error("Connection error: %s" % str(ex))
            return

        while True:

            time.sleep(1)

            # See if we can find any transfer requests
            treqs = um_col.find(
                {"_id": ObjectId(self.unit_manager_id)}, 
                {"output_transfer_queue": 1}
            )

            if treqs.count() != 1:
                logger.warning("Can't find DB entry for UnitManager %s", 
                    self.unit_manager_id)
                continue

            # Extract the transfer queue.
            output_transfer_queue = treqs[0]["output_transfer_queue"]
            logger.error("TF: %s" % output_transfer_queue)

            # Remove work unit ids from transfer queue
            um_col.update(
                {"_id": ObjectId(self.unit_manager_id)}, 
                {"$pullAll": { "output_transfer_queue": output_transfer_queue}}
            )



