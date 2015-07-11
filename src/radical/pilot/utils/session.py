
import os
import sys
import saga

import radical.utils as ru
from   radical.pilot.states import *

from db_utils import *

# ------------------------------------------------------------------------------
#
def fetch_profiles (sid, dburl=None, target=None):
    '''
    sid   : session for which all profiles are fetched
    target: dir to store the profile in

    returns list of file names
    '''

    ret = list()

    if not dburl:
        dburl = os.environ['RADICAL_PILOT_DBURL']

    if not dburl:
        raise RuntimeError ('Please set RADICAL_PILOT_DBURL')

    if not target:
        target = '.'

    _, db, _, _, _ = ru.mongodb_connect (dburl)

    json_docs = get_session_docs(db, sid)

    pilots = json_docs['pilot']
    num_pilots = len(pilots)
    print "Session: %s" % sid
    print "Number of pilots in session: %d" % num_pilots

    for pilot in pilots:

        print "Processing pilot '%s'" % pilot['_id']

        sandbox = pilot['sandbox']
        pilot_id = os.path.basename(os.path.dirname(sandbox))

        src = sandbox + 'agent.prof'

        if target.startswith('/'):
            dst = 'file://localhost/%s/%s.prof' % (target, pilot_id)
            ret.append('/%s/%s.prof' % (target, pilot_id))
        else:
            dst = 'file://localhost/%s/%s/%s.prof' % (os.getcwd(), target, pilot_id)
            ret.append('/%s/%s/%s.prof' % (os.getcwd(), target, pilot_id))

        print "Copying '%s' to '%s'." % (src, dst)
        prof_file = saga.filesystem.File(src)
        prof_file.copy(dst, flags=saga.filesystem.CREATE_PARENTS)
        prof_file.close()

    return ret


# ------------------------------------------------------------------------------

def fetch_session (sid, dburl=None, target=None) :

    '''
    returns file name
    '''

    if not dburl:
        dburl = os.environ['RADICAL_PILOT_DBURL']

    if not dburl:
        raise RuntimeError ('Please set RADICAL_PILOT_DBURL')

    if not target:
        target = '.'

    _, db, _, _, _ = ru.mongodb_connect (dburl)

    json_docs = get_session_docs(db, sid)

    if target.startswith('/'):
        dst = '/%s/%s.json' % (target, sid)
    else:
        dst = '/%s/%s/%s.json' % (os.getcwd(), target, sid)

    ru.write_json (json_docs, dst)
    print "session written to %s" % dst

    return dst


