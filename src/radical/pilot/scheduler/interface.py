#pylint: disable=C0301, C0103, W0212
"""
.. module:: radical.pilot.scheduler.Interface
   :platform: Unix
   :synopsis: The abstract interface class for all schedulers.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.pilot.utils.logger import logger

# -----------------------------------------------------------------------------
# 
class Scheduler(object):
    """Scheduler provides an abstsract interface for all schedulers.
    """

    # -------------------------------------------------------------------------
    # 
    def __init__(self, manager, session):
        """Le constructeur.
        """
        raise RuntimeError ('Not Implemented!')

    # -------------------------------------------------------------------------
    # 
    def pilot_added (self, pilot) :
        """Inform the scheduler about a new pilot"""

        logger.warn ("scheduler %s does not implement 'pilot_added()'" % self.name)

    # -------------------------------------------------------------------------
    # 
    def pilot_removed (self, pilot) :
        """Inform the scheduler about a pilot removal"""

        logger.warn ("scheduler %s does not implement 'pilot_removed()'" % self.name)

    # -------------------------------------------------------------------------
    # 
    def schedule (self, units) :
        """Schedules one or more ComputeUnits"""

        raise RuntimeError ("scheduler %s does not implement 'pilot_removed()'" % self.name)

    # -------------------------------------------------------------------------
    # 
    def unschedule (self, units) :
        """Unschedule one or more ComputeUnits"""

        logger.warn ("scheduler %s does not implement 'unschedule()'" % self.name)

    # -------------------------------------------------------------------------
    # 
    @property
    def name(self):
        """The name of the scheduler"""
        return self.__class__.__name__

