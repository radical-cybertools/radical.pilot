

import os
import sys
import threading
import subprocess

# ------------------------------------------------------------------------------
#

# import API classes as basis
from _api import *


# then load the selected backend -- which will overwrite as many api classes as
# it wants to
_sinon_backend = os.environ.get ('SINON_BACKEND', 'v2')

if  _sinon_backend == 'v1' :
    print 'sinon: using v1 backend'
    from   v1 import *
elif  _sinon_backend == 'v2' :
    print 'sinon: using v2 backend'
    from   v2 import *
elif  _sinon_backend == 'bj' :
    print 'sinon: using bj backend'
    from   bj import *


# ------------------------------------------------------------------------------
#
_rlock = threading.RLock ()



# ------------------------------------------------------------------------------
#
with _rlock :

    version = 'unknown'

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
                    version = v.group()
        except OSError:
            pass


# ------------------------------------------------------------------------------
#


