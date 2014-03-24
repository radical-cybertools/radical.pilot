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
    def __init__(self):
        """Le constructeur.
        """
        Scheduler.__init__(self)
        logger.info("Loaded scheduler: %s." % self.name)

        self._idx = 0

    # -------------------------------------------------------------------------
    #
    def __del__(self):
        """Le destructeur.
        """
        if os.getenv("RADICALPILOT_GCDEBUG", None) is not None:
            logger.debug("__del__(): %s." % self.name)

    # -------------------------------------------------------------------------
    #
    def _name(self):
        return "RoundRobinScheduler"

    # -------------------------------------------------------------------------
    #
    def schedule(self, manager, unit_descriptions):
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

        if  not manager :
            raise RuntimeError ('Unit scheduler is not initialized')

        pilots = manager.list_pilots ()
        ret    = dict()

        if not len (pilots) :
            raise RuntimeError ('Unit scheduler cannot operate on empty pilot set')


        for ud in unit_descriptions :
            
            if  self._idx >= len(pilots) : 
                self._idx = 0
            
            pilot = pilots[self._idx]

            if  pilot not in ret :
                ret[pilot] = []

            ret[pilot].append (ud)

            self._idx += 1


        return ret
