#!/usr/bin/env python3

__copyright__ = 'Copyright 2013-2016, http://radical.rutgers.edu'
__license__   = 'MIT'

import sys
import radical.utils as ru
import radical.pilot as rp

rpu = rp.utils


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    if len(sys.argv) <= 1:
        print("\n\tusage: %s <session_id>\n")
        sys.exit(1)

    sid = sys.argv[1]


    profiles = rpu.fetch_profiles(sid=sid, skip_existing=True)
    for p in profiles:
        print(p)

    profs = ru.read_profiles(profiles)

    for p in profs:
        print(type(p))

    prof = ru.combine_profiles(profs)

    print(len(prof))
    for entry in prof:
        print(entry)




# ------------------------------------------------------------------------------

