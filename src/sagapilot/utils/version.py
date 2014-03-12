#pylint: disable=C0301, C0103, W0212, E1101, R0903

"""
.. module:: sagapilot.utils.version
   :platform: Unix
   :synopsis: Implementation of the version extractor.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os

_mod_root = os.path.dirname (__file__)

version        = open (_mod_root + "/../VERSION",     "r").readline ().strip ()
version_detail = open (_mod_root + "/../VERSION.git", "r").readline ().strip ()

