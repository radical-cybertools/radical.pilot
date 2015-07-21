
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
        target = os.getcwd()
            
    if not target.startswith('/'):
        target = "%s/%s" % (os.getcwd, target)

    _, db, _, _, _ = ru.mongodb_connect (dburl)

    json_docs = get_session_docs(db, sid)

    pilots = json_docs['pilot']
    num_pilots = len(pilots)
    print "Session: %s" % sid
    print "Number of pilots in session: %d" % num_pilots

    for pilot in pilots:

        print "Processing pilot '%s'" % pilot['_id']

        sandbox  = saga.filesystem.Directory (pilot['sandbox'])
        profiles = sandbox.list('*.prof')

        for prof in profiles:

            ret.append('/%s/%s' % (target, prof))

            print "fetching '%s/%s' to '%s'." % (pilot['sandbox'], prof, target)
            prof_file = saga.filesystem.File("%s/%s" % (pilot['sandbox'], prof))
            prof_file.copy(target, flags=saga.filesystem.CREATE_PARENTS)
            prof_file.close()

    return ret


# ------------------------------------------------------------------------------

def fetch_json (sid, dburl=None, target=None) :

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


