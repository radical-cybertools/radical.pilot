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
        self._idx    = 0

        logger.info("Loaded scheduler: %s." % self.name)


    # -------------------------------------------------------------------------
    #
    def schedule(self, unit_descriptions):
        # the scheduler will return a dictionary of the form:
        #   { 
        #     pilot_id_1  : [ud_1, ud_2, ...], 
        #     pilot_id_2  : [ud_3, ud_4, ...], 
        #     ...
        #   }
        # The scheduler may not be able to schedule some units -- those will
        # simply not be listed for any pilot.  The UM needs to make sure
        # that no UD from the original list is left untreated, eventually.

        #print "round-robin scheduling of %s units" % len(unit_descriptions)

        pilot_ids = self.manager.list_pilots ()
        schedule  = dict()

        if not len (pilot_ids) :
            raise RuntimeError ('Unit scheduler cannot operate on empty pilot set')


        for unit in unit_descriptions :
            
            if  self._idx >= len(pilot_ids) : 
                self._idx = 0
            
            schedule[unit] = pilot_ids[self._idx]
            self._idx     += 1

        return schedule

