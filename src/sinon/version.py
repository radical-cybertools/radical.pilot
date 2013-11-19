"""
.. module:: sinon.version
   :platform: Unix
   :synopsis: Implementation of the version helper.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import threading
import subprocess


VERSION = 'unknown'

try:
    cwd = os.path.dirname(os.path.abspath(__file__))
    fn = os.path.join(cwd, 'VERSION')
    version = open(fn).read().strip()
except IOError:
    from subprocess import Popen, PIPE, STDOUT
    import re

    VERSION_MATCH = re.compile(r'\d+\.\d+\.\d+(\w|-)*')

    try:
        p = Popen(['git', 'describe', '--tags', '--always'],
                  stdout=PIPE, stderr=STDOUT)
        out = p.communicate()[0]

        if (not p.returncode) and out:
            v = VERSION_MATCH.search(out)
            if v:
                VERSION = v.group()
    except OSError:
        pass