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
from round_robin       import RoundRobinScheduler
from backfilling       import BackfillingScheduler

# -----------------------------------------------------------------------------
# Constants
SCHED_ROUND_ROBIN       = "round_robin"
SCHED_DIRECT            = "direct_submission"
SCHED_BACKFILLING       = "backfilling"

# alias:
SCHED_DIRECT_SUBMISSION = SCHED_DIRECT

# default:
SCHED_DEFAULT           = SCHED_ROUND_ROBIN

# -----------------------------------------------------------------------------
# 
def get_scheduler(manager, name, session):
    """get_scheduler returns a scheduler object for 'name'.
    """
    if   name == SCHED_ROUND_ROBIN      : return RoundRobinScheduler       (manager, session)
    elif name == SCHED_DIRECT_SUBMISSION: return DirectSubmissionScheduler (manager, session)
    elif name == SCHED_BACKFILLING      : return BackfillingScheduler      (manager, session)
    else                                : raise RuntimeError("Scheduler '%s' doesn't exist." % name)

