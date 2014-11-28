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


# ------------------------------------------------------------------------------
# The logentry "struct" encapsulates a log entry and its timestamp.
class Logentry(object):

    __slots__ = ('message', 'timestamp')

    # --------------------------------------------------------------------------
    #
    def __init__(self, message, timestamp=None, logger=None):

        if not timestamp :
            timestamp = datetime.datetime.utcnow()

        if  logger :
            logger (message)

        self.message   = message
        self.timestamp = timestamp


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def from_dict (d) :

        return Logentry (message=d['message'], timestamp=d['timestamp'])


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns the message and its timestamp as a Python dictionary.
        """
        return {"message"  : self.message, 
                "timestamp": self.timestamp}

    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns the string representation of the log entry. The string 
           representation is just the state as string without its timestamp.
        """
        return self.message

# ------------------------------------------------------------------------------

