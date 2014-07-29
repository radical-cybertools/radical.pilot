#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.scheduler.RoundRobinScheduler
   :platform: Unix
   :synopsis: A multi-pilot, round-robin scheduler.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 

from radical.pilot.utils.logger        import logger
from radical.pilot.scheduler.interface import Scheduler 

# -----------------------------------------------------------------------------
# 
class RoundRobinScheduler(Scheduler):
    """RoundRobinScheduler implements a multi-pilot, round-robin 
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
        self.idx     = 0

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

        # the scheduler will return a dictionary of the form:
        #   { 
        #     unit_1: pilot_id_1
        #     unit_2: pilot_id_2
        #     unit_4: pilot_id_2
        #     ...
        #   }
        # The scheduler may not be able to schedule some units -- those will
        # simply not be listed for any pilot.  The UM needs to make sure
        # that no UD from the original list is left untreated, eventually.

        pilot_ids          = self.pilots.keys ()
        schedule           = dict()
        schedule['units']  = dict()
        schedule['pilots'] = self.pilots

        if  not len (pilot_ids) :
            raise RuntimeError ('Unit scheduler cannot operate on empty pilot set')


        for unit in units :
            
            if  self.idx >= len(pilot_ids) : 
                self.idx = 0
            
            schedule['units'][unit] = pilot_ids[self.idx]
            self.idx               += 1


        return schedule


    # -------------------------------------------------------------------------

