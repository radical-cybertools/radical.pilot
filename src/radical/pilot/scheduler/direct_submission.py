#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.scheduler.DirectSubmissionScheduler
   :platform: Unix
   :synopsis: A single-pilot, direct submission scheduler.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os

from radical.pilot.utils.logger        import logger
from radical.pilot.scheduler.interface import Scheduler 

# -----------------------------------------------------------------------------
# 
class DirectSubmissionScheduler(Scheduler):
    """DirectSubmissionScheduler implements a single-pilot 'pass-through' 
    scheduling algorithm.
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self, manager, session):
        """Le constructeur.
        """

        self.manager = manager
        self.session = session

        logger.info("Loaded scheduler: %s." % self.name)

    # -------------------------------------------------------------------------
    #
    def schedule(self, units):

        pilot_ids = self.manager.list_pilots()

        if not len (pilot_ids):
            raise RuntimeError ('Unit scheduler cannot operate on empty pilot set')

        if len (pilot_ids) > 1:
            raise RuntimeError ('Direct Submission only works for a single pilot!')
        
        schedule = dict()
        for unit in units:

            schedule[unit] = pilot_ids[0]

        return schedule

