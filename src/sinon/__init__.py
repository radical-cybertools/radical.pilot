

import os
import sys
import threading
import subprocess


# ------------------------------------------------------------------------------
#
_rlock = threading.RLock ()


# ------------------------------------------------------------------------------
#
# load the right backend (default: v1)
#
with _rlock :

    _sinon_backend = 'v1'
    
    if  'SINON_BACKEND' in os.environ :
        _sinon_backend = os.environ['SINON_BACKEND']
    
    if  _sinon_backend == 'v1' :
        print 'sinon: using v1 backend'
        from   v1 import *
    elif  _sinon_backend == 'bj' :
        print 'sinon: using bj backend'
        from   bj import *
    else :
        print "sinon: unknown backend '%s'" % _sinon_backend
        sys.exit (0)



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
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

