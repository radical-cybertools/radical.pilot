"""
.. module:: radical.pilot.controller.pilot_launcher_worker
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

# BULK_LIMIT defines the max. number of transfer requests to pull from DB.
BULK_LIMIT=1

# ----------------------------------------------------------------------------
#
class PilotLauncherWorker(multiprocessing.Process):
    """PilotLauncherWorker handles bootstrapping and laucnhing of 
       the pilot agents.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, db_connection_info, pilot_manager_id, number=None):
        """Creates a new pilot launcher background process.
        """
        # Multiprocessing stuff
        multiprocessing.Process.__init__(self)
        self.daemon = True

        self.db_connection_info = db_connection_info
        self.pilot_manager_id = pilot_manager_id

        self.name = "PilotLauncherWorker-%s" % str(number)

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
            pilot_col = db["%s.p" % self.db_connection_info.session_id]
            logger.debug("Connected to MongoDB. Serving requests for PilotManager %s" % self.pilot_manager_id)

            session_col = db["%s" % self.db_connection_info.session_id]
            session = session_col.find(
                {"_id": ObjectId(self.db_connection_info.session_id)},
                {"credentials": 1}
            )

            for cred_dict in session[0]["credentials"]:
                cred = SSHCredential.from_dict(cred_dict)
                saga_session.add_context(cred._context)
                logger.debug("Found SSH context info: %s." % cred._context)

        except Exception, ex:
            tb = traceback.format_exc()
            logger.error("Connection error: %s. %s" % (str(ex), tb))
            return

        while True:
            compute_pilot = None

            # See if we can find a ComputePilot that is waiting to be launched.
            ts = datetime.datetime.utcnow()
            compute_unit = pilot_col.find_and_modify(
                query={"pilotmanager": self.pilot_manager_id,
                       "state" : PENDING_BOOTSTRAP},
                update={"$set" : {"state": BOOTSTRAPPING},
                        "$push": {"statehistory": {"state": BOOTSTRAPPING, "timestamp": ts}}},
                limit=BULK_LIMIT
            )

            if compute_pilot is None:
                # Sleep a bit if no new units are available.
                time.sleep(1)
            else:
                try:
                    # LAUNCH HERE
                    compute_pilot_id = str(compute_piplot["_id"])
                    logger.info("Launching ComputePilot %s" % compute_pilot_id)
                    time.sleep(2)

                    # Update the CU's state to 'DONE' if all transfers were successfull.
                    ts = datetime.datetime.utcnow()
                    pilot_col.update(
                        {"_id": ObjectId(compute_pilot_id)},
                        {"$set": {"state": PENDING_EXECUTION},
                         "$push": {"statehistory": {"state": PENDING_EXECUTION, "timestamp": ts}},
                         "$pushAll": {"log": log_messages}}                    
                    )

                except Exception, ex:
                    # Update the CU's state 'FAILED'.
                    ts = datetime.datetime.utcnow()
                    log_messages = "Pilot launching failed: %s" % str(ex)
                    pilot_col.update(
                        {"_id": ObjectId(compute_pilot_id)},
                        {"$set": {"state": FAILED},
                         "$push": {"statehistory": {"state": FAILED, "timestamp": ts}},
                         "$push": {"log": log_messages}}
                    )
