#pylint: disable=C0301, C0103, W0212, E1101, R0903

"""
.. module:: radical.pilot.utils.version
   :platform: Unix
   :synopsis: Implementation of the version extractor.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os

version        = open(os.path.dirname (os.path.abspath (__file__)) + "/../VERSION",     'r').read().strip()
version_detail = open(os.path.dirname (os.path.abspath (__file__)) + "/../VERSION.git", 'r').read().strip()

