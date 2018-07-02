
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
def get_session_ids(db) :

    # this is not bein cashed, as the session list can and will change freqently
    return db.collection_names(include_system_collections=False)


# ------------------------------------------------------------------------------
def get_last_session(db) :

    # this assumes that sessions are ordered by time -- which is the case at
    # this point...
    return get_session_ids(db)[-1]


# ------------------------------------------------------------------------------
def get_session_docs(db, sid, cache=None, cachedir=None) :

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
          # print 'using cache: %s' % cache
            return ru.read_json (cache)
    except Exception as e :
        # continue w/o cache
        sys.stderr.write ("warning: cannot read session cache at %s (%s)\n" % (cache, e))


    # cache not used or not found -- go to db
    json_data = dict()

    # convert bson to json, i.e. serialize the ObjectIDs into strings.
    json_data['session'] = bson2json(list(db[sid].find({'type' : 'session'})))
    json_data['pmgr'   ] = bson2json(list(db[sid].find({'type' : 'pmgr'   })))
    json_data['pilot'  ] = bson2json(list(db[sid].find({'type' : 'pilot'  })))
    json_data['umgr'   ] = bson2json(list(db[sid].find({'type' : 'umgr'   })))
    json_data['unit'   ] = bson2json(list(db[sid].find({'type' : 'unit'   })))

    if  len(json_data['session']) == 0 :
        raise ValueError ('no session %s in db (was `cleanup` disabled on `session.close()`?)' % sid)

  # if  len(json_data['session']) > 1 :
  #     print 'more than one session document -- picking first one'

    # there can only be one session, not a list of one
    json_data['session'] = json_data['session'][0]

    # we want to add a list of handled units to each pilot doc
    for pilot in json_data['pilot'] :

        pilot['unit_ids'] = list()

        for unit in json_data['unit'] :

            if  unit['pilot'] == pilot['uid']:
                pilot['unit_ids'].append (unit['uid'])

    # if we got here, we did not find a cached version -- thus add this dataset
    # to the cache
    try :
        os.system ('mkdir -p %s' % cachedir)
        ru.write_json (json_data, "%s/%s.json" % (cachedir, sid))
    except Exception as e :
        # we can live without cache, no problem...
        pass

    return json_data



# ------------------------------------------------------------------------------
#
def get_session_slothist(db, sid, cache=None, cachedir=None) :
    """
    For all pilots in the session, get the slot lists and slot histories. and
    return as list of tuples like:

            [pilot_id,      [      [hostname, slotnum] ],      [      [slotstate, timestamp] ] ] 
      tuple (string  , list (tuple (string  , int    ) ), list (tuple (string   , datetime ) ) )
    """

    docs = get_session_docs(db, sid, cache, cachedir)

    ret = dict()

    for pilot_doc in docs['pilot'] :

        pilot_id     = pilot_doc['uid'] 
        slot_names   = list()
        slot_infos   = dict()
        slot_started = dict()

        nodes   = pilot_doc['nodes']
        n_cores = pilot_doc['cores_per_node']

        if  not nodes:
          # print "no nodes in pilot doc for %s" % pilot_id
            continue

        for node in nodes :
            for core in range(n_cores):
                slot_name = "%s:%s" % (node, core)
                slot_names.append (slot_name)
                slot_infos  [slot_name] = list()
                slot_started[slot_name] = sys.maxint

        for unit_doc in docs['unit'] :
            if unit_doc['pilot'] == pilot_doc['uid'] :

                started  = None
                finished = None
                for event in sorted (unit_doc['state_history'], 
                                     key=lambda x: x['timestamp']) :
                    if started :
                        finished = event['timestamp']
                        break
                    if event['state'] == AGENT_EXECUTING :
                        started = event['timestamp']

                if not started or not finished :
                  # print "no start/finish for cu %s - ignored" % unit_doc['uid']
                    continue

                for slot_id in unit_doc['opaque_slots'] :
                    if slot_id not in slot_infos :
                      # print "slot %s for pilot %s unknown - ignored" % (slot_id, pilot_id)
                        continue
                    
                    slot_infos[slot_id].append([started, finished])
                    slot_started[slot_id] = min(started, slot_started[slot_id])

        for slot_id in slot_infos :
            slot_infos[slot_id].sort(key=lambda x: float(x[0]))

        # we use the startup time to sort the slot names, as that gives a nicer
        # representation when plotting.  That sorting should probably move to
        # the plotting tools though... (FIXME)
        slot_names.sort (key=lambda x: slot_started[x])

        ret[pilot_id] = dict()
        ret[pilot_id]['started']    = pilot_doc['started']
        ret[pilot_id]['finished']   = pilot_doc['finished']
        ret[pilot_id]['slots']      = slot_names
        ret[pilot_id]['slot_infos'] = slot_infos

    return ret


# ------------------------------------------------------------------------------
def get_session_events(db, sid, cache=None, cachedir=None) :
    """
    For all entities in the session, create simple event tuples, and return
    them as a list

           [      [object type, object id, pilot id, timestamp, event name, object document] ]
      list (tuple (string     , string   , string  , datetime , string    , dict           ) )
      
    """

    docs = get_session_docs(db, sid, cache, cachedir)

    ret = list()

    if  'session' in docs :
        doc   = docs['session']
        odoc  = dict()
        otype = 'session'
        oid   = doc['uid']
        ret.append (['state', otype, oid, None, doc['created'],   'created',   odoc])
        ret.append (['state', otype, oid, None, doc['connected'], 'connected', odoc])

    for doc in docs['pilot'] :
        odoc  = dict()
        otype = 'pilot'
        oid   = doc['uid']

        for event in [# 'submitted', 'started',    'finished',  # redundant to states..
                      'input_transfer_started',  'input_transfer_finished', 
                      'output_transfer_started', 'output_transfer_finished'] :
            if  event in doc :
                ret.append (['state', otype, oid, oid, doc[event], event, odoc])
            else : 
                ret.append (['state', otype, oid, oid, None,       event, odoc])

        for event in doc['state_history'] :
            ret.append (['state',     otype, oid, oid, event['timestamp'], event['state'], odoc])

        if  'callbackhistory' in doc :
            for event in doc['callbackhistory'] :
                ret.append (['callback',  otype, oid, oid, event['timestamp'], event['state'], odoc])


    for doc in docs['unit'] :
        odoc  = dict()
        otype = 'unit'
        oid   = doc['uid']
        pid   = doc['pilot']

        # TODO: change states to look for
        for event in [# 'submitted', 'started',    'finished',  # redundant to states..
                      'input_transfer_started',  'input_transfer_finished', 
                      'output_transfer_started', 'output_transfer_finished'
                      ] :
            if  event in doc :
                ret.append (['state', otype, oid, pid, doc[event], event, doc])
            else :                                
                ret.append (['state', otype, oid, pid, None,       event, doc])

        for event in doc['state_history'] :
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

