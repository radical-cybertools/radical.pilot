
__copyright__ = "Copyright 2013-2015, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import thread
import threading

from ..states import * 
from ..utils  import logger
from ..utils  import timestamp
from ..staging_directives import CREATE_PARENTS

IDLE_TIME  = 1.0  # seconds to sleep after idle cycles

# ----------------------------------------------------------------------------
#
class OutputFileTransferWorker(threading.Thread):
    """OutputFileTransferWorker handles the staging of the output files
    for a UnitManagerController.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, session, unit_manager_id, number=None):

        self._session = session

        # threading stuff
        threading.Thread.__init__(self)
        self.daemon = True

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

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the process when Process.start() is called.
        """

        # make sure to catch sys.exit (which raises SystemExit)
        try :

            # Try to connect to the database and create a tailable cursor.
            try:
                db = self._session.get_db()
                um_col = db["%s.cu" % self._session.uid]
                logger.debug("Connected to MongoDB. Serving requests for UnitManager %s." % self.unit_manager_id)

            except Exception as e:
                logger.exception("Connection error: %s" % e)
                return

            while not self._stop.is_set() and \
                  not self._session._terminate.is_set():

                # See if we can find a ComputeUnit that is waiting for client output file transfer.
                # FIXME: this method is not bulkable.  See agent pulling for
                #        units for an approach to split the call into two bulkable 
                #        ones.
                ts = timestamp()
                compute_unit = um_col.find_and_modify(
                    query={"unitmanager": self.unit_manager_id,
                           "state"      : PENDING_OUTPUT_STAGING,
                           "control"    : 'agent'},
                    update={"$set" : {"state"   : STAGING_OUTPUT,
                                      "control" : 'umgr'},
                            "$push": {"statehistory" : 
                                         {"state"    : STAGING_OUTPUT, 
                                          "timestamp": ts}
                                     }
                            })

                if compute_unit is None:
                    # Sleep a bit if no new units are available.
                    time.sleep(IDLE_TIME)
                else:
                    logger.info("OFTW CU found, progressing ...")
                    state = STAGING_OUTPUT
                    compute_unit_id = None
                    try:
                        log_messages = []

                        # We have found a new CU. Now we can process the transfer
                        # directive(s) with SAGA.
                        compute_unit_id = str(compute_unit["_id"])

                        self._session.prof.prof('advance', uid=compute_unit_id,
                                msg=STAGING_OUTPUT, state=STAGING_OUTPUT)
                        logger.debug ("OutputStagingController: unit found: %s" % compute_unit_id)

                        remote_sandbox = compute_unit["sandbox"]
                        output_staging = compute_unit.get("FTW_Output_Directives", [])

                        logger.info("OutputStagingController: Processing output file transfers for ComputeUnit %s" % compute_unit_id)
                        # Loop over all staging directives and execute them.
                        for sd in output_staging:

                            logger.debug("OutputStagingController: sd: %s : %s" % (compute_unit_id, sd))

                            # Check if there was a cancel request for this CU
                            # TODO: Can't these cancel requests come from a central place?
                            state_doc = um_col.find_one(
                                {"_id": compute_unit_id},
                                fields=["state"]
                            )
                            if state_doc['state'] == CANCELED:
                                logger.info("Compute Unit Canceled, interrupting output file transfers.")
                                self._session.prof.prof('advance', uid=compute_unit_id, 
                                        msg=CANCELED, state=CANCELED)
                                state = CANCELED
                                # Break out of the loop over all SD's, into the loop over CUs
                                break

                            abs_src = "%s/%s" % (remote_sandbox, sd['source'])

                            if os.path.basename(sd['target']) == sd['target']:
                                abs_target = "file://localhost%s" % os.path.join(os.getcwd(), sd['target'])
                            else:
                                abs_target = "file://localhost%s" % os.path.abspath(sd['target'])

                            log_msg = "Transferring output file %s -> %s" % (abs_src, abs_target)
                            log_messages.append(log_msg)
                            logger.debug(log_msg)

                            output_file = saga.filesystem.File(saga.Url(abs_src),
                                                               session=self._session)

                            if CREATE_PARENTS in sd['flags']:
                                copy_flags = saga.filesystem.CREATE_PARENTS
                            else:
                                copy_flags = 0

                            try:
                                output_file.copy(saga.Url(abs_target), flags=copy_flags)
                                output_file.close()
                            except Exception as e:
                                logger.exception(e)
                                raise Exception("copy failed(%s)" % e.message)

                        # If the CU was canceled we can skip the remainder of this loop,
                        # and return to the CU loop
                        if state == CANCELED:
                            continue

                        # Update the CU's state to 'DONE'.
                        ts = timestamp()
                        log_message = "Output transfer completed."
                        um_col.update({'_id': compute_unit_id}, {
                            '$set': {'state': DONE},
                            '$push': {
                                'statehistory': {'state': DONE, 'timestamp': ts},
                                'log': {'message': log_message, 'timestamp': ts}
                            }
                        })
                        self._session.prof.prof('advance', uid=compute_unit_id,
                                msg=DONE, state=DONE)

                    except Exception as e :
                        # Update the CU's state to 'FAILED'.
                        ts = timestamp()
                        log_message = "Output transfer failed: %s" % e
                        um_col.update({'_id': compute_unit_id}, {
                            '$set': {'state': FAILED},
                            '$push': {
                                'statehistory': {'state': FAILED, 'timestamp': ts},
                                'log': {'message': log_message, 'timestamp': ts}
                            }})
                        logger.exception(log_message)
                        self._session.prof.prof('advance', uid=compute_unit_id, 
                                msg=FAILED, state=FAILED)
                        raise

        except SystemExit as e :
            logger.exception("output file transfer thread caught system exit -- forcing application shutdown")
            thread.interrupt_main()
