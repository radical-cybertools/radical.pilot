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
        self.pilots  = dict()

        logger.info("Loaded scheduler: %s." % self.name)

    # -------------------------------------------------------------------------
    #
    def pilot_added (self, pilot) :

        pid = pilot.uid

        self.pilots[pid] = dict()
        self.pilots[pid]['resource'] = pilot.resource
        self.pilots[pid]['sandbox']  = pilot.sandbox


    # -------------------------------------------------------------------------
    #
    def pilot_removed (self, pid) :

        if  not pid in self.pilots :
            raise RuntimeError ('cannot remove unknown pilot (%s)' % pid)

        del self.pilots[pid]


    # -------------------------------------------------------------------------
    #
    def schedule(self, units):

        pilot_ids = self.pilots.keys ()

        if not len (pilot_ids):
            raise RuntimeError ('Unit scheduler cannot operate on empty pilot set')

        if len (pilot_ids) > 1:
            raise RuntimeError ('Direct Submission only works for a single pilot!')
        
        schedule           = dict()
        schedule['units']  = dict()
        schedule['pilots'] = self.pilots

        for unit in units:

            schedule['units'][unit] = pilot_ids[0]

        return schedule

