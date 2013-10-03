
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import os
import sys


# ------------------------------------------------------------------------------
#
version = "latest"

try :
    cwd     = os.path.dirname (os.path.abspath (__file__))
    fn      = os.path.join    (cwd, '../VERSION')
    version = open (fn).read ().strip ()

    
    from   subprocess import Popen, PIPE, STDOUT
    import re

    VERSION_MATCH = re.compile(r'\d+\.\d+\.\d+(\w|-)*')

    p   = Popen (['git', 'describe', '--tags', '--always'],
                 stdout=PIPE, stderr=STDOUT)
    out = p.communicate()[0]


    # ignore pylint error on p.returncode -- false positive
    if  out and not p.returncode :
        v = VERSION_MATCH.search (out)

        if  None != v :
            version = v
        else :
            version += '-' + out

except IOError :
    pass

except OSError :
    pass

# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

