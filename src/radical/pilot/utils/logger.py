#pylint: disable=C0301, C0103, W0212, E1101, R0903

"""
.. module:: radical.pilot.utils.logger
   :platform: Unix
   :synopsis: Implementation of the logging facility.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import logging

from   radical.utils.singleton import Singleton

import radical.utils        as ru
import radical.utils.logger as rul


# -----------------------------------------------------------------------------
#
class _MPLogger(object):
    """Singleton class to initialize custom multiprocessing logger.
    """
    __metaclass__ = Singleton

    def __init__(self):
        """Create or get a new logger instance (singleton).
        """
        self._logger = rul.logger.getLogger(name='radical.pilot')

    def get(self):
        """Return the logger.
        """
        return self._logger


def get_logger(name, target, level):

    logger    = logging.getLogger  (name)
    handle    = logging.FileHandler(target)
    formatter = logging.Formatter  ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.setLevel(level)
    handle.setFormatter(formatter)
    logger.addHandler(handle)

    import radical.utils as ru
    import saga          as rs

    pwd  = os.path.dirname (__file__)
    root = "%s/.." % pwd
    rp_version, _, _, _, _ = ru.get_version ([root, pwd])

    logger.info("Using RADICAL-Utils version %s", rs.version)
    logger.info("Using RADICAL-SAGA  version %s", rs.version)
    logger.info("Using RADICAL-Pilot version %s", rp_version)

    return logger

# -----------------------------------------------------------------------------
#
logger = _MPLogger().get()
logger = rul.logger.getLogger(name='radical.pilot')

