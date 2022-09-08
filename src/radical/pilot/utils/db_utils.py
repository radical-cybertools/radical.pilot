
import datetime
import json
import os
import sys
import time

import radical.utils as ru

_CACHE_BASEDIR = '/tmp/rp_cache_%d/' % os.getuid()


# ------------------------------------------------------------------------------
#
def bson2json (bson_data):

    # thanks to
    # http://stackoverflow.com/questions/16586180/ \
    #                          typeerror-objectid-is-not-json-serializable

    from   bson.objectid import ObjectId

    class MyJSONEncoder (json.JSONEncoder):
        def default (self, o):
            if  isinstance (o, ObjectId):
                return str (o)
            if  isinstance (o, datetime.datetime):
                seconds  = time.mktime (o.timetuple ())
                seconds += (o.microsecond / 1000000.0)
                return seconds
            return json.JSONEncoder.default (self, o)

    return ru.parse_json (MyJSONEncoder ().encode (bson_data))


# ------------------------------------------------------------------------------
#
def get_session_ids(db):

    # this is not bein cashed, as the session list can and will change freqently
    return db.collection_names(include_system_collections=False)


# ------------------------------------------------------------------------------
def get_last_session(db):

    # this assumes that sessions are ordered by time -- which is the case at
    # this point...
    return get_session_ids(db)[-1]


# ------------------------------------------------------------------------------
def get_session_docs(sid, db=None, cachedir=None):

    # session docs may have been cached in /tmp/rp_cache_<uid>/<sid>.json -- in
    # that case we pull it from there instead of the database, which will be
    # much quicker.  Also, we do cache any retrieved docs to that place, for
    # later use.  An optional cachdir parameter changes that default location
    # for lookup and storage.

    if not cachedir:
        cachedir = _CACHE_BASEDIR

    cache = os.path.join(cachedir, '%s.json' % sid)
    try:
        if os.path.isfile(cache):
            return ru.read_json(cache)
    except Exception as e:
        # continue w/o cache
        sys.stderr.write('cannot read session cache from %s: %s\n' % (cache, e))

    if db:
        json_data = dict()
        # convert bson to json, i.e. serialize the ObjectIDs into strings.
        json_data['session'] = bson2json(list(db[sid].find({'type': 'session'})))
        json_data['pmgr'   ] = bson2json(list(db[sid].find({'type': 'pmgr'   })))
        json_data['pilot'  ] = bson2json(list(db[sid].find({'type': 'pilot'  })))
        json_data['tmgr'   ] = bson2json(list(db[sid].find({'type': 'tmgr'   })))
        json_data['task'   ] = bson2json(list(db[sid].find({'type': 'task'   })))

        if  len(json_data['session']) == 0:
            raise ValueError ('no session %s in db' % sid)
    else:
        import glob
        jsons = glob.glob('%s/r[ep].session.*.json' % os.path.abspath(sid))
        assert jsons, 'session docs missed, check <sid>/<sid>.json'
        session_json = jsons[0]
        with ru.ru_open(session_json, 'r') as f:
            json_data = json.load(f)

    # we want to add a list of handled tasks to each pilot doc
    for pilot in json_data['pilot']:

        pilot['task_ids'] = list()

        for task in json_data['task']:

            if task['pilot'] == pilot['uid']:
                pilot['task_ids'].append(task['uid'])

    # if we got here, we did not find a cached version -- thus add this dataset
    # to the cache
    try:
        os.system('mkdir -p %s' % cachedir)
        ru.write_json(json_data, cache)
    except:
        # we can live without cache, no problem...
        pass

    return json_data


# ------------------------------------------------------------------------------
def get_session_events(sid, cachedir=None):
    """
    For all entities in the session, create simple event tuples, and return
    them as a list

    [     [otype , oid   , pid   , timestamp, event name, odoc]]
    list(tuple(string, string, string, datetime , string    , dict))

    """

    docs = get_session_docs(sid, cachedir=cachedir)
    ret  = list()

    if  'session' in docs:
        doc   = docs['session']
        odoc  = dict()
        otype = 'session'
        oid   = doc['uid']
        ret.append(['state', otype, oid, None, doc['created'],
                    'created',   odoc])
        ret.append(['state', otype, oid, None, doc['connected'],
                    'connected', odoc])

    for doc in docs['pilot']:
        odoc  = dict()
        otype = 'pilot'
        oid   = doc['uid']

        for event in ['input_transfer_started',  'input_transfer_finished',
                      'output_transfer_started', 'output_transfer_finished']:
            if  event in doc:
                ret.append (['state', otype, oid, oid, doc[event], event, odoc])
            else:
                ret.append (['state', otype, oid, oid, None,       event, odoc])

        if  'callbackhistory' in doc:
            for event in doc['callbackhistory']:
                ret.append (['callback',  otype, oid, oid, event['timestamp'],
                             event['state'], odoc])


    for doc in docs['task']:
        odoc  = dict()
        otype = 'task'
        oid   = doc['uid']
        pid   = doc['pilot']

        # TODO: change states to look for
        for event in ['input_transfer_started',  'input_transfer_finished',
                      'output_transfer_started', 'output_transfer_finished']:

            if  event in doc:
                ret.append (['state', otype, oid, pid, doc[event], event, doc])
            else:
                ret.append (['state', otype, oid, pid, None,       event, doc])

        # TODO: this probably needs to be "doc"
        if  'callbackhistory' in event:
            for event in doc['callbackhistory']:
                ret.append (['callback',  otype, oid, pid, event['timestamp'],
                             event['state'], doc])

    # we don't want None times, actually
    for r in list(ret):
        if  r[4] is None:
            ret.remove (r)

    ret.sort (key=lambda tup: tup[4])

    return ret


# ------------------------------------------------------------------------------

