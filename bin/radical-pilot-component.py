#!/usr/bin/env python

__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import sys

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":
    '''
    This script starts an RP component in a separate, detached process: it is
    run after a low-level fork/exec, attempting to avoid the many processing and
    threading pitfalls of python.  The component is manually deleted after its
    `serve()`method completed - we do not rely on garbage collection.
    '''

    try:
        print "start %s" % sys.argv
        component = None  # enable the test in the finally clause
        component = rp.utils.EVComponent.create(sys.argv)
        sys.exit(component.serve())

    finally:
        print "stop  %s" % sys.argv
        if component: del(component)


# ------------------------------------------------------------------------------

