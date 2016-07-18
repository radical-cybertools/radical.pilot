#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

import radical.pilot as rp
import radical.utils as ru

rpu = rp.utils


#------------------------------------------------------------------------------
#
if __name__ == '__main__':

    if len(sys.argv) <= 1:
        print "\n\tusage: %s <session_id>\n"
        sys.exit(1)

    sid = sys.argv[1]


    profs = rpu.fetch_profiles(sid=sid, dburl=None, client=os.getcwd(),
                               tgt=os.getcwd(), access=None, skip_existing=True)
    for p in profs:
        print p

    prof = rpu.combine_profiles(profs)
    print len(prof)
    for entry in prof:
        print entry




#-------------------------------------------------------------------------------

