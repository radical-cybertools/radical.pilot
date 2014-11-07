#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.logentry
   :platform: Unix
   :synopsis: log entry wrapper

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import datetime

# -----------------------------------------------------------------------------
# The logentry "struct" encapsulates a log entry and its timestamp.
class Logentry(object):
    __slots__ = ('logentry', 'timestamp')

    # ----------------------------------------
    #
    def __init__(self, logentry, timestamp):
        """Le constructeur.
        """
        self.logentry = logentry
        self.timestamp = timestamp

    # ----------------------------------------
    #
    def as_dict(self):
        """Returns the logentry and its timestamp as a Python dictionary.
        """
        return {"logentry": self.logentry, "timestamp": self.timestamp}

    # ----------------------------------------
    #
    def __str__(self):
        """Returns the string representation of the log entry. The string 
           representation is just the state as string without its timestamp.
        """
        return self.logentry
