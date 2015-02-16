"""
.. module:: radical.pilot.controller.input_file_transfer_worker
.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import threading

from radical.pilot.states import * 
from radical.pilot.utils.logger import logger
from radical.pilot.staging_directives import CREATE_PARENTS

BULK_LIMIT = 1    # max. number of transfer requests to pull from DB.
IDLE_TIME  = 1.0  # seconds to sleep after idle cycles

# ----------------------------------------------------------------------------
#
class InputFileTransferWorker(threading.Thread):
    """InputFileTransferWorker handles the staging of input files
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
        self.name = "InputFileTransferWorker-%s" % str(self._worker_number)

        # we cache saga directories for performance, to speed up sandbox
        # creation.
        self._saga_dirs = dict()

        # Stop event can be set to terminate the main loop
        self._stop = threading.Event()
        self._stop.clear()


    # ------------------------------------------------------------------------
    #
    def stop(self):
        """stop() signals the process to finish up and terminate.
        """
        logger.debug("itransfer %s stopping" % (self.name))
        self._stop.set()
        self.join()
        logger.debug("itransfer %s stopped" % (self.name))
      # logger.debug("Worker thread (ID: %s[%s]) for UnitManager %s stopped." %
      #             (self.name, self.ident, self.unit_manager_id))


    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the process when Process.start() is called.
        """

        # make sure to catch sys.exit (which raises SystemExit)
        try :

            logger.info("Starting InputFileTransferWorker")

            # Try to connect to the database and create a tailable cursor.
            try:
                connection = self.db_connection_info.get_db_handle()
                db = connection[self.db_connection_info.dbname]
                um_col = db["%s.cu" % self.db_connection_info.session_id]
                logger.debug("Connected to MongoDB. Serving requests for UnitManager %s." % self.unit_manager_id)

            except Exception as e :
                logger.exception("Connection error: %s" % e)
                raise

            try :
                while not self._stop.is_set():
                    # See if we can find a ComputeUnit that is waiting for
                    # input file transfer.
                    compute_unit = None

                    ts = datetime.datetime.utcnow()
                    compute_unit = um_col.find_and_modify(
                        query={"unitmanager": self.unit_manager_id,
                               "FTW_Input_Status": PENDING},
                        update={"$set" : {"FTW_Input_Status": EXECUTING,
                                          "state": STAGING_INPUT},
                                "$push": {"statehistory": {"state": STAGING_INPUT, "timestamp": ts}}},
                        limit=BULK_LIMIT # TODO: bulklimit is probably not the best way to ensure there is just one
                    )
                    # FIXME: AM: find_and_modify is not bulkable!
                    state = STAGING_INPUT

                    if compute_unit is None:
                        # Sleep a bit if no new units are available.
                        time.sleep(IDLE_TIME) 

                    else:
                        compute_unit_id = None
                        try:
                            log_messages = []

                            # We have found a new CU. Now we can process the transfer
                            # directive(s) wit SAGA.
                            compute_unit_id = str(compute_unit["_id"])
                            remote_sandbox = compute_unit["sandbox"]
                            input_staging = compute_unit["FTW_Input_Directives"]

                            # We need to create the CU's directory in case it doesn't exist yet.
                            log_msg = "Creating ComputeUnit sandbox directory %s." % remote_sandbox
                            log_messages.append(log_msg)
                            logger.info(log_msg)

                            # Creating the sandbox directory.
                            try:
                                logger.debug ("saga.fs.Directory ('%s')" % remote_sandbox)

                                remote_sandbox_keyurl = saga.Url (remote_sandbox)
                                remote_sandbox_keyurl.path = '/'
                                remote_sandbox_key = str(remote_sandbox_keyurl)

                                if  remote_sandbox_key not in self._saga_dirs :
                                    self._saga_dirs[remote_sandbox_key] = \
                                            saga.filesystem.Directory (remote_sandbox_key,
                                                    flags=saga.filesystem.CREATE_PARENTS,
                                                    session=self._session)

                                saga_dir = self._saga_dirs[remote_sandbox_key]
                                saga_dir.make_dir (remote_sandbox, 
                                                   flags=saga.filesystem.CREATE_PARENTS)
                            except Exception as e :
                                logger.exception('Error: %s' % e)
                                # FIXME: why is this exception ignored?  AM


                            logger.info("Processing input file transfers for ComputeUnit %s" % compute_unit_id)
                            # Loop over all transfer directives and execute them.
                            for sd in input_staging:

                                state_doc = um_col.find_one(
                                    {"_id": compute_unit_id},
                                    fields=["state"]
                                )
                                if state_doc['state'] == CANCELED:
                                    logger.info("Compute Unit Canceled, interrupting input file transfers.")
                                    state = CANCELED
                                    break

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
                                logger.debug ("saga.fs.File ('%s')" % input_file_url)
                                input_file = saga.filesystem.File(
                                    input_file_url,
                                    session=self._session
                                )

                                if CREATE_PARENTS in sd['flags']:
                                    copy_flags = saga.filesystem.CREATE_PARENTS
                                else:
                                    copy_flags = 0

                                try :
                                    input_file.copy(target, flags=copy_flags)
                                except Exception as e :
                                    logger.exception (e)
                                input_file.close()

                                # If all went fine, update the state of this StagingDirective to Done
                                um_col.find_and_modify(
                                    query={"_id" : compute_unit_id,
                                           'FTW_Input_Status': EXECUTING,
                                           'FTW_Input_Directives.state': PENDING,
                                           'FTW_Input_Directives.source': sd['source'],
                                           'FTW_Input_Directives.target': sd['target'],
                                           },
                                    update={'$set': {'FTW_Input_Directives.$.state': 'Done'},
                                            '$push': {'log': {
                                                'timestamp': datetime.datetime.utcnow(), 
                                                'message'  : log_msg}}
                                    }
                                )

                        except Exception as e :
                            # Update the CU's state 'FAILED'.
                            ts = datetime.datetime.utcnow()
                            logentry = {'message'  : "Input transfer failed: %s" % e,
                                        'timestamp': ts}

                            um_col.update({'_id': compute_unit_id}, {
                                '$set': {'state': FAILED},
                                '$push': {
                                    'statehistory': {'state': FAILED, 'timestamp': ts},
                                    'log': logentry
                                }
                            })

                            logger.exception(str(logentry))

                    # Code below is only to be run by the "first" or only worker
                    if self._worker_number > 1:
                        continue

                    # If the CU was canceled we can skip the remainder of this loop.
                    if state == CANCELED:
                        continue

                    #
                    # Check to see if there are more pending Directives, if not, we are Done
                    #
                    cursor_w = um_col.find({"unitmanager": self.unit_manager_id,
                                            "$or": [ {"Agent_Input_Status": EXECUTING},
                                                     {"FTW_Input_Status": EXECUTING}
                                                   ]
                                            }
                                           )
                    # Iterate over all the returned CUs (if any)
                    for cu in cursor_w:
                        # See if there are any FTW Input Directives still pending
                        if cu['FTW_Input_Status'] == EXECUTING and \
                                not any(d['state'] == EXECUTING or d['state'] == PENDING for d in cu['FTW_Input_Directives']):
                            # All Input Directives for this FTW are done, mark the CU accordingly
                            um_col.update({"_id": cu["_id"]},
                                          {'$set': {'FTW_Input_Status': DONE},
                                           '$push': {'log': {
                                                'timestamp': datetime.datetime.utcnow(),
                                                'message'  : 'All FTW Input Staging Directives done - %d.' % self._worker_number}}
                                           }
                            )

                        # See if there are any Agent Input Directives still pending or executing,
                        # if not, mark it DONE.
                        if cu['Agent_Input_Status'] == EXECUTING and \
                                not any(d['state'] == EXECUTING or d['state'] == PENDING for d in cu['Agent_Input_Directives']):
                            # All Input Directives for this Agent are done, mark the CU accordingly
                            um_col.update({"_id": cu["_id"]},
                                           {'$set': {'Agent_Input_Status': DONE},
                                            '$push': {'log': {
                                                'timestamp': datetime.datetime.utcnow(), 
                                                'message'  : 'All Agent Input Staging Directives done - %d.' % self._worker_number}}
                                           }
                            )

                    #
                    # Check for all CUs if both Agent and FTW staging is done, we can then mark the CU PendingExecution
                    #
                    ts = datetime.datetime.utcnow()
                    um_col.find_and_modify(
                        query={"unitmanager": self.unit_manager_id,
                               "Agent_Input_Status": { "$in": [ None, DONE ] },
                               "FTW_Input_Status": { "$in": [ None, DONE ] },
                               "state": STAGING_INPUT
                        },
                        update={"$set": {
                                    "state": PENDING_EXECUTION
                                },
                                "$push": {
                                    "statehistory": {"state": PENDING_EXECUTION, "timestamp": ts}
                                }
                        }
                    )

            except Exception as e :

                logger.exception("transfer worker error: %s" % e)
                self._session.close (cleanup=False)
                raise

        except SystemExit as e :
            logger.debug("input file transfer thread caught system exit -- forcing application shutdown")
            import thread
            thread.interrupt_main ()
