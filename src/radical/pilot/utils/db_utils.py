
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
    ret['pilots' ] = list(database["%s.p"  % session].find ())
    ret['umgr'   ] = list(database["%s.wm" % session].find ())
    ret['units'  ] = list(database["%s.w"  % session].find ())

    return ret


# ------------------------------------------------------------------------------
def get_session_slothist (dbclient, dbname, session) :
    """
    For all pilots in the session, get the slot lists and slot histories. and
    return as list of tuples like:

            [pilot_id,      [      [hostname, slotnum] ],      [      [slotstate, timestamp] ] ] 
      tuple (string  , list (tuple (string  , int    ) ), list (tuple (string   , datetime ) ) )
    """

    docs = get_session_docs (dbclient, dbname, "%s.p" % session)

    ret = list()

    for doc in docs['session'] :
        pid   = str(doc['_id'])
        slots =     doc['slots']
        hist  =     doc['slothistory']

        slotinfo = list()
        for hostinfo in slots :
            hostname = hostinfo['host'] 
            slotnum  = len (hostinfo['cores'])
            slotinfo.append ([hostname, slotnum])

        ret.append ([pid, slotinfo, hist])

  # import pprint
  # pprint.pprint (ret)

    return ret


# ------------------------------------------------------------------------------
def get_session_events (dbclient, dbname, session) :
    """
    For all entities in the session, create simple event tuples, and return
    them.  A tuple is

      [object type, object id, timestamp, event name, object document]
    """

    docs = get_session_docs (dbclient, dbname, session)

    ret = list()

    for doc in docs['session'] :
        odoc  = dict()
        otype = 'session'
        oid   = str(doc['_id'])
        ret.append ([otype, oid, doc['created'],        'created',        odoc])
        ret.append ([otype, oid, doc['last_reconnect'], 'last_reconnect', odoc])

    for doc in docs['pilots'] :
        odoc  = dict()
        otype = 'pilot'
        oid   = str(doc['_id'])

        for event in ['submitted', 'started',    'finished',
                      'input_transfer_started',  'input_transfer_finished', 
                      'output_transfer_started', 'output_transfer_finished'] :
            if  event in doc :
                ret.append ([otype, oid, doc[event], event, odoc])
            else : 
                ret.append ([otype, oid, None,       event, odoc])

        for event in doc['statehistory'] :
            ret.append ([otype, oid, event['timestamp'], event['state'], odoc])


    for doc in docs['units'] :
        odoc  = dict()
        otype = 'unit'
        oid   = str(doc['_id'])

        for event in ['submitted', 'started',    'finished',
                      'input_transfer_started',  'input_transfer_finished', 
                      'output_transfer_started', 'output_transfer_finished'
                      ] :
            if  event in doc :
                ret.append ([otype, oid, doc[event], event, odoc])
            else : 
                ret.append ([otype, oid, None,       event, odoc])

        for event in doc['statehistory'] :
            ret.append ([otype, oid, event['timestamp'], event['state'], odoc])

    # we don't want None times, actually
    for r in list(ret) :
        if  r[2] == None :
            ret.remove (r)
          # r[2] =  datetime.datetime.min

    ret.sort (key=lambda tup: tup[2]) 

    return ret


# ------------------------------------------------------------------------------

