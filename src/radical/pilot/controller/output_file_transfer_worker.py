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

        self._worker_number = number
        self.name = "OutputFileTransferWorker-%s" % str(self._worker_number)

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
                       "FTW_Output_Status": PENDING},
                update={"$set" : {"FTW_Output_Status": EXECUTING,
                                  "state": STAGING_OUTPUT},
                        "$push": {"statehistory": {"state": STAGING_OUTPUT, "timestamp": ts}}},
                limit=BULK_LIMIT
            )

            logger.info("OFTW after finding pending wus")
            if compute_unit is None:
                logger.info("OFTW no wus, sleep")
                # Sleep a bit if no new units are available.
                time.sleep(1)
            else:
                logger.info("OFTW wu found, progressing ...")
                # AM: The code below seems wrong when BULK_LIMIT != 1 -- the
                # compute_unit will be a list then I assume.
                try:
                    log_messages = []

                    # We have found a new CU. Now we can process the transfer
                    # directive(s) wit SAGA.
                    compute_unit_id = str(compute_unit["_id"])
                    remote_sandbox = compute_unit["sandbox"]
                    staging_directives = compute_unit["description"]["output_staging"]

                    logger.info("Processing output file transfers for ComputeUnit %s" % compute_unit_id)
                    # Loop over all staging directives and execute them.
                    for sd in staging_directives:

                        action = sd['action']
                        source = sd['source']
                        target = sd['target']

                        # Mark the beginning of transfer this StagingDirective
                        um_col.find_and_modify(
                            query={"_id" : ObjectId(compute_unit_id),
                                   'FTW_Output_Status': EXECUTING,
                                   'FTW_Output_Directives.state': PENDING,
                                   'FTW_Output_Directives.source': sd['source'],
                                   'FTW_Output_Directives.target': sd['target'],
                                   },
                            update={'$set': {'FTW_Output_Directives.$.state': EXECUTING},
                                    '$push': {'log': 'Starting transfer of %s' % source}
                            }
                        )

                        abs_source = "%s/%s" % (remote_sandbox, source)

                        if os.path.basename(target) == target:
                            abs_target = "file://localhost%s" % os.path.join(os.getcwd(), target)
                        else:
                            abs_target = "file://localhost%s" % os.path.abspath(target)

                        log_msg = "Transferring output file %s -> %s" % (abs_source, abs_target)
                        logmessage = "Transferred output file %s -> %s" % (abs_source, abs_target)
                        log_messages.append(log_msg)
                        logger.debug(log_msg)

                        output_file = saga.filesystem.File(saga.Url(abs_source),
                            session=self._session
                        )
                        output_file.copy(saga.Url(abs_target))
                        output_file.close()

                        # If all went fine, update the state of this StagingDirective to Done
                        um_col.find_and_modify(
                            query={"_id" : ObjectId(compute_unit_id),
                                   'FTW_Output_Status': EXECUTING,
                                   'FTW_Output_Directives.state': EXECUTING,
                                   'FTW_Output_Directives.source': sd['source'],
                                   'FTW_Output_Directives.target': sd['target'],
                                   },
                            update={'$set': {'FTW_Output_Directives.$.state': DONE},
                                    '$push': {'log': logmessage}
                            }
                        )


                except Exception, ex:
                    # Update the CU's state 'FAILED'.
                    ts = datetime.datetime.utcnow()
                    log_messages = "Output transfer failed: %s\n%s" % (str(ex), traceback.format_exc())
                    # TODO: not only mark the CU as failed, but also the specific Directive
                    um_col.update(
                        {"_id": ObjectId(compute_unit_id)},
                        {"$set": {"state": FAILED},
                         "$push": {"statehistory": {"state": FAILED, "timestamp": ts}},
                         "$push": {"log": log_messages}}
                    )
                    logger.error(log_messages)

            # Code below is only to be run by the "first" or only worker
            if self._worker_number > 1:
                continue

            #
            # Check to see if there are more active Directives, if not, we are Done
            #
            cursor_w = um_col.find({"unitmanager": self.unit_manager_id,
                                    "$or": [ {"Agent_Output_Status": EXECUTING},
                                             {"FTW_Output_Status": EXECUTING}
                                    ]
            }
            )
            # Iterate over all the returned CUs (if any)
            for wu in cursor_w:
                # See if there are any FTW Output Directives still pending
                if wu['FTW_Output_Status'] == EXECUTING and \
                        not any(d['state'] == EXECUTING or d['state'] == PENDING for d in wu['FTW_Output_Directives']):
                    # All Output Directives for this FTW are done, mark the WU accordingly
                    um_col.update({"_id": ObjectId(wu["_id"])},
                                  {'$set': {'FTW_Output_Status': DONE},
                                   '$push': {'log': 'All FTW output staging directives done - %d.' % self._worker_number}})

                # See if there are any Agent Output Directives still pending
                if wu['Agent_Output_Status'] == EXECUTING and \
                        not any(d['state'] == EXECUTING or d['state'] == PENDING for d in wu['Agent_Output_Directives']):
                    # All Output Directives for this Agent are done, mark the WU accordingly
                    um_col.update({"_id": ObjectId(wu["_id"])},
                                  {'$set': {'Agent_Output_Status': DONE},
                                   '$push': {'log': 'All Agent Output Staging Directives done-%d.' % self._worker_number}
                                  })

            #
            # Check for all CUs if both Agent and FTW staging is done, we can then mark the CU Done
            #
            ts = datetime.datetime.utcnow()
            um_col.find_and_modify(
                query={"unitmanager": self.unit_manager_id,
                       "Agent_Output_Status": { "$in": [ NULL, DONE ] },
                       "FTW_Output_Status": { "$in": [ NULL, DONE ] },
                       "state": STAGING_OUTPUT
                },
                update={"$set": {
                    "state": DONE
                },
                        "$push": {
                            "statehistory": {"state": DONE, "timestamp": ts}
                        }
                }
            )

