"""
.. module:: radical.pilot.mpworker
   :platform: Unix
   :synopsis: The multiprocessing workers for RADICAL-Pilot.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

from unit_manager_controller  import UnitManagerController
from pilot_manager_worker import PilotManagerWorker

from output_file_transfer_worker import OutputFileTransferWorker