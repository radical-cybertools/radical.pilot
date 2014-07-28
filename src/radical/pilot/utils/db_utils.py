
import datetime
import pymongo

# ------------------------------------------------------------------------------
#
def get_session_ids (dbclient, dbname) :

    if not dbname : usage ("require specific database name to list session IDs")

    database = dbclient[dbname]
    cnames = database.collection_names ()

    sids = list()
    for cname in cnames :
        if  not '.' in cname :
            sids.append (cname)

    return sids


# ------------------------------------------------------------------------------
def get_last_session (dbclient, dbname) :

    # this assumes that sessions are ordered by time -- which is the case at
    # this point...
    return get_session_ids (dbclient, dbname)[-1]


# ------------------------------------------------------------------------------
def get_session_docs (dbclient, dbname, session) :

    database = dbclient[dbname]

    ret = dict()

    ret['session'] = list(database["%s"    % session].find ())
    ret['pmgr'   ] = list(database["%s.pm" % session].find ())
    ret['pilot'  ] = list(database["%s.p"  % session].find ())
    ret['umgr'   ] = list(database["%s.wm" % session].find ())
    ret['unit'   ] = list(database["%s.w"  % session].find ())

    return ret


# ------------------------------------------------------------------------------
def get_session_slothist (dbclient, dbname, session) :
    """
    For all pilots in the session, get the slot lists and slot histories. and
    return as list of tuples like:

            [pilot_id,      [      [hostname, slotnum] ],      [      [slotstate, timestamp] ] ] 
      tuple (string  , list (tuple (string  , int    ) ), list (tuple (string   , datetime ) ) )
    """

    docs = get_session_docs (dbclient, dbname, session)

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
def get_session_events (dbclient, dbname, session) :
    """
    For all entities in the session, create simple event tuples, and return
    them as a list

           [      [object type, object id, pilot id, timestamp, event name, object document] ]
      list (tuple (string     , string   , string  , datetime , string    , dict           ) )
      
    """

    docs = get_session_docs (dbclient, dbname, session)

    ret = list()

    for doc in docs['session'] :
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

        if  'callbackhistory' in event :
            for event in doc['callbackhistory'] :
                ret.append (['callback',  otype, oid, oid, event['timestamp'], event['state'], odoc])


    for doc in docs['unit'] :
        odoc  = dict()
        otype = 'unit'
        oid   = str(doc['_id'])
        pid   = str(doc['pilot'])

        for event in [# 'submitted', 'started',    'finished',  # redundant to states..
                      'input_transfer_started',  'input_transfer_finished', 
                      'output_transfer_started', 'output_transfer_finished'
                      ] :
            if  event in doc :
                ret.append (['state', otype, oid, pid, doc[event], event, odoc])
            else :                                
                ret.append (['state', otype, oid, pid, None,       event, odoc])

        for event in doc['statehistory'] :
            ret.append (['state',     otype, oid, pid, event['timestamp'], event['state'], odoc])

        if  'callbackhistory' in event :
            for event in doc['callbackhistory'] :
                ret.append (['callback',  otype, oid, pid, event['timestamp'], event['state'], odoc])

    # we don't want None times, actually
    for r in list(ret) :
        if  r[4] == None :
            ret.remove (r)
    
    ret.sort (key=lambda tup: tup[4]) 

    return ret


# ------------------------------------------------------------------------------

