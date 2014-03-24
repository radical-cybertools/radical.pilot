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

# -----------------------------------------------------------------------------
# Constants
SCHED_ROUND_ROBIN       = "round_robin"
SCHED_DIRECT_SUBMISSION = "direct_submission"

# -----------------------------------------------------------------------------
# 
def get_scheduler(name):
    """get_scheduler returns a scheduler object for 'name'.
    """
    if name == SCHED_ROUND_ROBIN:
        return RoundRobinScheduler()

    elif name == SCHED_DIRECT_SUBMISSION:
        return DirectSubmissionScheduler()

    else:
        raise RuntimeError("Scheduler '%s' doesn't exist." % name)
