#pylint: disable=C0301, C0103, W0212
"""
.. module:: radical.pilot.scheduler.Interface
   :platform: Unix
   :synopsis: The abstract interface class for all schedulers.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

# -----------------------------------------------------------------------------
# 
class Scheduler(object):
    """Scheduler provides an abstsract interface for all schedulers.
    """

    # -------------------------------------------------------------------------
    # 
    def __init__(self):
        """Le constructeur.
        """

    # -------------------------------------------------------------------------
    # 
    def __del__(self):
        """Le destructeur.
        """
        pass

    # -------------------------------------------------------------------------
    # 
    def _name(self):
        """PRIVATE: Returns the name of the scheduler.
        """
        raise RuntimeError ('Not Implemented!')

    # -------------------------------------------------------------------------
    # 
    def schedule(self, manager, unit_descriptions):
        """Schedules one or more ComputeUnits.
        """
        raise RuntimeError ('Not Implemented!')

    # -------------------------------------------------------------------------
    # 
    @property
    def name(self):
        """The name of the scheduler.
        """
        return self._name()
