#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.scheduler
   :platform: Unix
   :synopsis: Scheduler implementations.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

from direct_submission import DirectSubmissionScheduler
from round_robin import RoundRobinScheduler
from late_binding import LateBindingScheduler

# -----------------------------------------------------------------------------
# Constants
SCHED_ROUND_ROBIN       = "round_robin"
SCHED_DIRECT_SUBMISSION = "direct_submission"
SCHED_LATE_BINDING      = "late_binding"

# -----------------------------------------------------------------------------
# 
def get_scheduler(manager, name):
    """get_scheduler returns a scheduler object for 'name'.
    """
    if   name == SCHED_ROUND_ROBIN      : return RoundRobinScheduler       (manager) 
    elif name == SCHED_DIRECT_SUBMISSION: return DirectSubmissionScheduler (manager) 
    elif name == SCHED_LATE_BINDING     : return LateBindingScheduler      (manager)
    else                                : raise RuntimeError("Scheduler '%s' doesn't exist." % name)

