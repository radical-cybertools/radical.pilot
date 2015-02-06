
import os
import sys
import time
import datetime
import pymongo

import radical.utils as ru
from   radical.pilot.states import *


_CACHE_BASEDIR = '/tmp/rp_cache_%d/' % os.getuid ()


# ------------------------------------------------------------------------------
#
def bson2json (bson_data) :

    # thanks to
    # http://stackoverflow.com/questions/16586180/typeerror-objectid-is-not-json-serializable

    import json
    from   bson.objectid import ObjectId

    class MyJSONEncoder (json.JSONEncoder) :
        def default (self, o):
            if  isinstance (o, ObjectId) :
                return str (o)
            if  isinstance (o, datetime.datetime) :
                seconds  = time.mktime (o.timetuple ())
                seconds += (o.microsecond / 1000000.0) 
                return seconds
            return json.JSONEncoder.default (self, o)

    return ru.parse_json (MyJSONEncoder ().encode (bson_data))


# ------------------------------------------------------------------------------
#
def get_session_ids (db) :

    # this is not bein cashed, as the session list can and will change freqently

    cnames = db.collection_names ()
    sids   = list()
    for cname in cnames :
        if  not '.' in cname :
            sids.append (cname)

    return sids


# ------------------------------------------------------------------------------
def get_last_session (db) :

    # this assumes that sessions are ordered by time -- which is the case at
    # this point...
    return get_session_ids (db)[-1]


# ------------------------------------------------------------------------------
def get_session_docs (db, sid, cache=None, cachedir=None) :

    # session docs may have been cached in /tmp/rp_cache_<uid>/<sid>.json -- in that
    # case we pull it from there instead of the database, which will be much
    # quicker.  Also, we do cache any retrieved docs to that place, for later
    # use.  An optional cachdir parameter changes that default location for
    # lookup and storage.
    if  not cachedir :
        cachedir = _CACHE_BASEDIR

    if  not cache :
        cache = "%s/%s.json" % (cachedir, sid)

    try :
        if  os.path.isfile (cache) :
            return ru.read_json (cache)
    except Exception as e :
        # continue w/o cache
        sys.stderr.write ("warning: cannot read session cache at %s (%s)\n" % (cache, e))


    # cache not used or not found -- go to db
    json_data = dict()

    # convert bson to json, i.e. serialize the ObjectIDs into strings.
    json_data['session'] = bson2json (list(db["%s"    % sid].find ()))
    json_data['pmgr'   ] = bson2json (list(db["%s.pm" % sid].find ()))
    json_data['pilot'  ] = bson2json (list(db["%s.p"  % sid].find ()))
    json_data['umgr'   ] = bson2json (list(db["%s.um" % sid].find ()))
    json_data['unit'   ] = bson2json (list(db["%s.cu" % sid].find ()))

    if  len(json_data['session']) == 0 :
        raise ValueError ('no such session %s' % sid)

  # if  len(json_data['session']) > 1 :
  #     print 'more than one session document -- picking first one'

    # there can only be one session, not a list of one
    json_data['session'] = json_data['session'][0]

    # we want to add a list of handled units to each pilot doc
    for pilot in json_data['pilot'] :

        pilot['unit_ids'] = list()

        for unit in json_data['unit'] :

            if  unit['pilot'] == str(pilot['_id']) :
                pilot['unit_ids'].append (str(unit['_id']))

    # if we got here, we did not find a cached version -- thus add this dataset
    # to the cache
    try :
        os.system ('mkdir -p %s' % _CACHE_BASEDIR)
        ru.write_json (json_data, "%s/%s.json" % (_CACHE_BASEDIR, sid))
    except Exception as e :
        # we can live without cache, no problem...
        pass

    return json_data



# ------------------------------------------------------------------------------
#
def get_session_frames (db, sids, cachedir=None) :

    # use like this: 
    #
    # session_frame, pilot_frame, unit_frame = rpu.get_session_frames (db, session, cachedir)
    # pandas.set_option('display.width', 1000)
    # print session_frame
    # print pilot_frame
    # print unit_frame
    #
    # u_min = unit_frame.ix[unit_frame['started'].idxmin()]['started']
    # u_max = unit_frame.ix[unit_frame['finished'].idxmax()]['finished']
    # print u_min
    # print u_max
    # print u_max - u_min


    if not isinstance (sids, list) :
        sids = [sids]

    session_dicts = list()
    pilot_dicts   = list()
    unit_dicts    = list()

    for sid in sids :

        docs = get_session_docs (db, sid, cachedir=cachedir)

        session       = docs['session']
        session_start = session['created']
        session_dict  = {
            'sid'       : sid,
            'started'   : session['created'],
            'finished'  : None, 
            'n_pilots'  : len(docs['pilot']),
            'n_units'   : 0
            }

        last_pilot_event = 0
        for pilot in docs['pilot'] :

            pid         = pilot['_id']
            description = pilot.get ('description', dict())
            started     = pilot.get ('started')
            finished    = pilot.get ('finished')

            if started  : started  -= session_start
            if finished : finished -= session_start

            pilot_dict = {
                'sid'          : sid,
                'pid'          : pid, 
                'n_units'      : len(pilot.get ('unit_ids', list())), 
                'started'      : started,
                'finished'     : finished,
                'resource'     : description.get ('resource'),
                'cores'        : description.get ('cores'),
                'runtime'      : description.get ('runtime'),
                NEW            : None, 
                PENDING        : None, 
                PENDING_LAUNCH : None, 
                LAUNCHING      : None, 
                PENDING_ACTIVE : None, 
                ACTIVE         : None, 
                DONE           : None, 
                FAILED         : None, 
                CANCELED       : None
            }

            for entry in pilot.get('statehistory', list()):
                state = entry['state']
                timer = entry['timestamp'] - session_start
                pilot_dict[state] = timer
                last_pilot_event  = max(last_pilot_event, timer)

            pilot_dicts.append (pilot_dict)


        for unit in docs['unit']:

            uid         = unit['_id']
            started     = unit.get ('started')
            finished    = unit.get ('finished')
            description = unit.get ('description', dict())

            if started  : started  -= session_start
            if finished : finished -= session_start

            unit_dict = {
                'sid'                  : sid, 
                'pid'                  : unit.get('pilot'), 
                'uid'                  : uid, 
                'started'              : started,
                'finished'             : finished,
                'cores'                : description.get ('cores'),
                NEW                    : None, 
                UNSCHEDULED            : None, 
                PENDING                : None, 
                PENDING_INPUT_STAGING  : None, 
                STAGING_INPUT          : None, 
                PENDING_EXECUTION      : None, 
                SCHEDULING             : None, 
                EXECUTING              : None, 
                PENDING_OUTPUT_STAGING : None, 
                STAGING_OUTPUT         : None, 
                DONE                   : None, 
                FAILED                 : None, 
                CANCELED               : None
            }

            for entry in unit.get('statehistory', list()):
                state = entry['state']
                timer = entry['timestamp'] - session_start
                unit_dict[state] = timer

            unit_dicts.append (unit_dict)
        
        session_dict['finished'] = last_pilot_event
        session_dicts.append (session_dict)

    import pandas 
    session_frame = pandas.DataFrame (session_dicts)
    pilot_frame   = pandas.DataFrame (pilot_dicts)
    unit_frame    = pandas.DataFrame (unit_dicts)


    return session_frame, pilot_frame, unit_frame


# ------------------------------------------------------------------------------
#
def get_session_slothist (db, sid, cache=None, cachedir=None) :
    """
    For all pilots in the session, get the slot lists and slot histories. and
    return as list of tuples like:

            [pilot_id,      [      [hostname, slotnum] ],      [      [slotstate, timestamp] ] ] 
      tuple (string  , list (tuple (string  , int    ) ), list (tuple (string   , datetime ) ) )
    """

    docs = get_session_docs (db, sid, cache, cachedir)

    ret = list()

    for pilot_doc in docs['pilot'] :

        # slot configuration was only recently (v0.18) added to the RP agent...
        if  not 'slots' in pilot_doc :
            return None

        pid      = str(pilot_doc['_id'])
        slots    =     pilot_doc['slots']
        slothist =     pilot_doc['slothistory']

        slotinfo = list()
        for hostinfo in slots :
            hostname = hostinfo['node'] 
            slotnum  = len (hostinfo['cores'])
            slotinfo.append ([hostname, slotnum])

        ret.append ({'pilot_id' : pid, 
                     'slotinfo' : slotinfo, 
                     'slothist' : slothist})

    return ret


# ------------------------------------------------------------------------------
def get_session_events (db, sid, cache=None, cachedir=None) :
    """
    For all entities in the session, create simple event tuples, and return
    them as a list

           [      [object type, object id, pilot id, timestamp, event name, object document] ]
      list (tuple (string     , string   , string  , datetime , string    , dict           ) )
      
    """

    docs = get_session_docs (db, sid, cache, cachedir)

    ret = list()

    if  'session' in docs :
        doc   = docs['session']
        odoc  = dict()
        otype = 'session'
        oid   = str(doc['_id'])
        ret.append (['state', otype, oid, None, doc['created'],        'created',        odoc])
        ret.append (['state', otype, oid, None, doc['last_reconnect'], 'last_reconnect', odoc])

    for doc in docs['pilot'] :
        odoc  = dict()
        otype = 'pilot'
        oid   = str(doc['_id'])

        for event in [# 'submitted', 'started',    'finished',  # redundant to states..
                      'input_transfer_started',  'input_transfer_finished', 
                      'output_transfer_started', 'output_transfer_finished'] :
            if  event in doc :
                ret.append (['state', otype, oid, oid, doc[event], event, odoc])
            else : 
                ret.append (['state', otype, oid, oid, None,       event, odoc])

        for event in doc['statehistory'] :
            ret.append (['state',     otype, oid, oid, event['timestamp'], event['state'], odoc])

        if  'callbackhistory' in doc :
            for event in doc['callbackhistory'] :
                ret.append (['callback',  otype, oid, oid, event['timestamp'], event['state'], odoc])


    for doc in docs['unit'] :
        odoc  = dict()
        otype = 'unit'
        oid   = str(doc['_id'])
        pid   = str(doc['pilot'])

        # TODO: change states to look for
        for event in [# 'submitted', 'started',    'finished',  # redundant to states..
                      'input_transfer_started',  'input_transfer_finished', 
                      'output_transfer_started', 'output_transfer_finished'
                      ] :
            if  event in doc :
                ret.append (['state', otype, oid, pid, doc[event], event, doc])
            else :                                
                ret.append (['state', otype, oid, pid, None,       event, doc])

        for event in doc['statehistory'] :
            ret.append (['state',     otype, oid, pid, event['timestamp'], event['state'], doc])

        # TODO: this probably needs to be "doc"
        if  'callbackhistory' in event :
            for event in doc['callbackhistory'] :
                ret.append (['callback',  otype, oid, pid, event['timestamp'], event['state'], doc])

    # we don't want None times, actually
    for r in list(ret) :
        if  r[4] == None :
            ret.remove (r)
    
    ret.sort (key=lambda tup: tup[4]) 

    return ret


# ------------------------------------------------------------------------------

