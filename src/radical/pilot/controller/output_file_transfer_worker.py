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
import threading

from radical.pilot.states import * 
from radical.pilot.utils.logger import logger
from radical.pilot.staging_directives import CREATE_PARENTS

BULK_LIMIT = 1    # max. number of transfer requests to pull from DB.
IDLE_TIME  = 1.0  # seconds to sleep after idle cycles

# ----------------------------------------------------------------------------
#
class OutputFileTransferWorker(threading.Thread):
    """OutputFileTransferWorker handles the staging of the output files
    for a UnitManagerController.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, session, db_connection_info, unit_manager_id, number=None):

        self._session = session

        # threading stuff
        threading.Thread.__init__(self)
        self.daemon = True

        self.db_connection_info = db_connection_info
        self.unit_manager_id = unit_manager_id

        self._worker_number = number
        self.name = "OutputFileTransferWorker-%s" % str(self._worker_number)

        # Stop event can be set to terminate the main loop
        self._stop = threading.Event()
        self._stop.clear()


    # ------------------------------------------------------------------------
    #
    def stop(self):
        """stop() signals the process to finish up and terminate.
        """
        logger.debug("otransfer %s stopping" % (self.name))
        self._stop.set()
        self.join()
        logger.debug("otransfer %s stopped" % (self.name))
      # logger.debug("Worker thread (ID: %s[%s]) for UnitManager %s stopped." %
      #             (self.name, self.ident, self.unit_manager_id))

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the process when Process.start() is called.
        """

        # make sure to catch sys.exit (which raises SystemExit)
        try :

            # Try to connect to the database and create a tailable cursor.
            try:
                connection = self.db_connection_info.get_db_handle()
                db = connection[self.db_connection_info.dbname]
                um_col = db["%s.cu" % self.db_connection_info.session_id]
                logger.debug("Connected to MongoDB. Serving requests for UnitManager %s." % self.unit_manager_id)

            except Exception as e:
                logger.exception("Connection error: %s" % e)
                return

            while not self._stop.is_set():
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
                # FIXME: AM: find_and_modify is not bulkable!
                state = STAGING_OUTPUT

                #logger.info("OFTW after finding pending cus")
                if compute_unit is None:
                    #logger.info("OFTW no cus, sleep")
                    # Sleep a bit if no new units are available.
                    time.sleep(IDLE_TIME)
                else:
                    logger.info("OFTW cu found, progressing ...")
                    compute_unit_id = None
                    try:
                        # We have found a new CU. Now we can process the transfer
                        # directive(s) wit SAGA.
                        compute_unit_id = str(compute_unit["_id"])
                        remote_sandbox = compute_unit["sandbox"]
                        staging_directives = compute_unit["FTW_Output_Directives"]

                        logger.info("Processing output file transfers for ComputeUnit %s" % compute_unit_id)
                        # Loop over all staging directives and execute them.
                        for sd in staging_directives:

                            # Check if there was a cancel request
                            state_doc = um_col.find_one(
                                {"_id": compute_unit_id},
                                fields=["state"]
                            )
                            if state_doc['state'] == CANCELED:
                                logger.info("Compute Unit Canceled, interrupting output file transfers.")
                                state = CANCELED
                                break

                            action = sd['action']
                            source = sd['source']
                            target = sd['target']
                            flags  = sd['flags']

                            # Mark the beginning of transfer this StagingDirective
                            um_col.find_and_modify(
                                query={"_id" : compute_unit_id,
                                       'FTW_Output_Status': EXECUTING,
                                       'FTW_Output_Directives.state': PENDING,
                                       'FTW_Output_Directives.source': sd['source'],
                                       'FTW_Output_Directives.target': sd['target'],
                                       },
                                update={'$set': {'FTW_Output_Directives.$.state': EXECUTING},
                                        '$push': {'log': {
                                            'timestamp': datetime.datetime.utcnow(),
                                            'message'  : 'Starting transfer of %s' % source}}
                                }
                            )

                            abs_source = "%s/%s" % (remote_sandbox, source)

                            if os.path.basename(target) == target:
                                abs_target = "file://localhost%s" % os.path.join(os.getcwd(), target)
                            else:
                                abs_target = "file://localhost%s" % os.path.abspath(target)

                            log_msg = "Transferring output file %s -> %s" % (abs_source, abs_target)
                            logger.debug(log_msg)

                            logger.debug ("saga.fs.File ('%s')" % saga.Url(abs_source))
                            output_file = saga.filesystem.File(saga.Url(abs_source),
                                session=self._session
                            )

                            if CREATE_PARENTS in flags:
                                copy_flags = saga.filesystem.CREATE_PARENTS
                            else:
                                copy_flags = 0
                            logger.debug ("saga.fs.File.copy ('%s')" % saga.Url(abs_target))
                            output_file.copy(saga.Url(abs_target), flags=copy_flags)
                            output_file.close()

                            # If all went fine, update the state of this StagingDirective to Done
                            um_col.find_and_modify(
                                query={"_id" : compute_unit_id,
                                       'FTW_Output_Status': EXECUTING,
                                       'FTW_Output_Directives.state': EXECUTING,
                                       'FTW_Output_Directives.source': sd['source'],
                                       'FTW_Output_Directives.target': sd['target'],
                                       },
                                update={'$set': {'FTW_Output_Directives.$.state': DONE},
                                        '$push': {'log': {
                                            'timestamp': datetime.datetime.utcnow(),
                                            'message'  : log_msg}}
                                }
                            )

                    except Exception as e :
                        # Update the CU's state to 'FAILED'.
                        ts = datetime.datetime.utcnow()
                        log_message = "Output transfer failed: %s" % e
                        # TODO: not only mark the CU as failed, but also the specific Directive
                        um_col.update({'_id': compute_unit_id}, {
                            '$set': {'state': FAILED},
                            '$push': {
                                'statehistory': {'state': FAILED, 'timestamp': ts},
                                'log': {'message': log_message, 'timestamp': ts}
                            }
                        })
                        logger.exception (log_message)


                # Code below is only to be run by the "first" or only worker
                if self._worker_number > 1:
                    continue

                # If the CU was canceled we can skip the remainder of this loop.
                if state == CANCELED:
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
                for cu in cursor_w:
                    # See if there are any FTW Output Directives still pending
                    if cu['FTW_Output_Status'] == EXECUTING and \
                            not any(d['state'] == EXECUTING or d['state'] == PENDING for d in cu['FTW_Output_Directives']):
                        # All Output Directives for this FTW are done, mark the CU accordingly
                        um_col.update({"_id": cu["_id"]},
                                      {'$set': {'FTW_Output_Status': DONE},
                                       '$push': {'log': {
                                           'timestamp': datetime.datetime.utcnow(),
                                           'message'  : 'All FTW output staging directives done - %d.' % self._worker_number}}
                                       }
                        )

                    # See if there are any Agent Output Directives still pending
                    if cu['Agent_Output_Status'] == EXECUTING and \
                            not any(d['state'] == EXECUTING or d['state'] == PENDING for d in cu['Agent_Output_Directives']):
                        # All Output Directives for this Agent are done, mark the CU accordingly
                        um_col.update({"_id": cu["_id"]},
                                      {'$set': {'Agent_Output_Status': DONE},
                                       '$push': {'log': {
                                           'timestamp': datetime.datetime.utcnow(),
                                           'message'  : 'All Agent Output Staging Directives done-%d.' % self._worker_number}}
                                      }
                        )

                #
                # Check for all CUs if both Agent and FTW staging is done, we can then mark the CU Done
                #
                ts = datetime.datetime.utcnow()
                um_col.find_and_modify(
                    query={"unitmanager": self.unit_manager_id,
                           # TODO: Now that our state model is linear,
                           # we probably don't need to check Agent_Output_Status anymore.
                           # Given that it is not updates by the agent currently, disable it here.
                           #"Agent_Output_Status": { "$in": [ None, DONE ] },
                           "FTW_Output_Status": { "$in": [ None, DONE ] },
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

        except SystemExit as e :
            logger.exception("output file transfer thread caught system exit -- forcing application shutdown")
            import thread
            thread.interrupt_main ()

