"""
.. module:: sinon.mpworker.UnitManagerWorker
   :platform: Unix
   :synopsis: Implements a multiprocessing worker backend for 
              the UnitManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

#pylint: disable=C0301, C0103
#pylint: disable=W0212

import os
import time
import saga
import datetime
import multiprocessing
from Queue import Empty


from radical.utils import which

import sagapilot.states     as states
import sagapilot.exceptions as exceptions


# ----------------------------------------------------------------------------
#
class UnitManagerWorker(multiprocessing.Process):
    """UnitManagerWorker is a multiprocessing worker that handles backend 
       interaction for the UnitManager class.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, logger, unitmanager_id, db_connection):

        # Multiprocessing stuff
        multiprocessing.Process.__init__(self)
        self.daemon  = True

        self._stop   = multiprocessing.Event()
        self._stop.clear()

        self.logger  = logger
        self._um_id  = unitmanager_id
        self._db     = db_connection

        # The different command queues hold pending operations
        # that are passed to the worker. Command queues are inspected during 
        # runtime in the run() loop and the worker acts upon them accordingly. 
        
        #self._cancel_pilot_requests  = multiprocessing.Queue()
        #self._startup_pilot_requests = multiprocessing.Queue()

    # ------------------------------------------------------------------------
    #
    def stop(self):
        """stop() signals the process to finish up and terminate. 
        """
        self._stop.set()
        self.join()
        self.logger.info("Worker process (PID: %s) for UnitManager %s stopped." % (self.pid, self._um_id))

    # ------------------------------------------------------------------------
    #
    def run(self):
        """run() is called when the process is started via 
           PilotManagerWorker.start().
        """
        self.logger.info("Worker process for UnitManager %s started with PID %s." % (self._um_id, self.pid))

        while not self._stop.is_set():
            time.sleep(1)
