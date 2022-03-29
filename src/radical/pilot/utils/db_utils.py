
# pylint: disable=cell-var-from-loop

import os
import json
import time
import datetime

import radical.utils as ru


_CACHE_BASEDIR = '/tmp/rp_cache_%d/' % os.getuid ()


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
def get_session_docs(db, sid_path, cache=None, cachedir=None):

    # session docs may have been cached in /tmp/rp_cache_<uid>/<sid>.json -- in
    # that case we pull it from there instead of the database, which will be
    # much quicker.  Also, we do cache any retrieved docs to that place, for
    # later use.  An optional cachdir parameter changes that default location
    # for lookup and storage.

    import glob
    path = "%s/rp.session.*.json" % os.path.abspath(sid_path)
    session_json = glob.glob(path)[0]
    with open(session_json, 'r') as f:
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
        os.system ('mkdir -p %s' % cachedir)
        ru.write_json (json_data, "%s/%s.json" % (cachedir, sid_path))
    except Exception:
        # we can live without cache, no problem...
        pass

    return json_data


# ------------------------------------------------------------------------------
def get_session_events(db, sid, cache=None, cachedir=None):
    """
    For all entities in the session, create simple event tuples, and return
    them as a list

    [     [otype , oid   , pid   , timestamp, event name, odoc]]
    list(tuple(string, string, string, datetime , string    , dict))

    """

    docs = get_session_docs(db, sid, cache, cachedir)
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

