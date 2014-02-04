"""
.. module:: sagapilot.utils.logger
   :platform: Unix
   :synopsis: Implementation of the logging facility.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

from   radical.utils.singleton import Singleton
import radical.utils.logger as rul
from   logging import Formatter

# ------------------------------------------------------------------------------
#
class _MPLogger(object):
    """Singleton class to initialize custom multiprocessing logger.
    """
    __metaclass__ = Singleton

    def __init__(self):
        """Create or get a new logger instance (singleton).
        """
        self._logger = rul.logger.getLogger ('sagapilot')
        mp_formatter = Formatter(fmt='%(asctime)s %(processName)-20s %(name)-22s: [%(levelname)-8s] %(message)s', 
                                 datefmt='%Y:%m:%d %H:%M:%S')

        for handler in self._logger.handlers:
            handler.setFormatter(mp_formatter)

    def get(self):
        """Return the logger.
        """
        return self._logger

# ------------------------------------------------------------------------------
#
logger = _MPLogger().get()
