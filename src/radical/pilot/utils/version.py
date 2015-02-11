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

import radical.utils        as ru
import radical.utils.logger as rul


pwd     = os.path.dirname (__file__)
root    = "%s/.." % pwd
version, version_detail, version_branch = ru.get_version ([root, pwd])

# FIXME: the logger init will require a 'classical' ini based config, which is
# different from the json based config we use now.   May need updating once the
# radical configuration system has changed to json
_logger = rul.logger.getLogger  ('radical.pilot')
_logger.info ('radical.pilot        version: %s' % version_detail)

sdist          = open(os.path.dirname (os.path.abspath (__file__)) + "/../SDIST",       'r').read().strip()
sdist_path     = "%s/../%s" % (os.path.dirname (os.path.abspath (__file__)), sdist)

