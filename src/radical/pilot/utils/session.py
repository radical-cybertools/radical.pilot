
import os
import sys
import glob
import saga

import radical.utils as ru
from   radical.pilot.states import *

from db_utils import *

# ------------------------------------------------------------------------------
#
def fetch_profiles (sid, dburl=None, src=None, tgt=None, session=None):
    '''
    sid: session for which all profiles are fetched
    src: dir to look for session profiles
    tgt: dir to store the profile in

    returns list of file names
    '''

    ret = list()

    if not dburl:
        dburl = os.environ['RADICAL_PILOT_DBURL']

    if not dburl:
        raise RuntimeError ('Please set RADICAL_PILOT_DBURL')

    if not src:
        src = os.getcwd()
            
    if not tgt:
        tgt = os.getcwd()
            
    if not tgt.startswith('/') and '://' not in tgt:
        tgt = "%s/%s" % (os.getcwd(), tgt)

    # we always create a session dir as real target
    tgt = "%s/%s/" % (tgt, sid)

    # at the moment, we only support localhost as fetch target
    tgt_url = saga.Url(tgt)
    if not saga.utils.misc.url_is_local (tgt_url):
        raise ValueError('Only local fetch targets are supported (%s)' % tgt_url)

    # make locality explicit
    tgt_url.schema = 'file'
    tgt_url.host   = 'localhost'

    # first fetch session profile
    # FIXME: should we record pwd or profile location in db session?  Or create
    #        a sandbox like dir for storing profiles and logs?
    profiles = glob.glob("%s/%s.prof" % (src, sid))

    if not profiles:
        raise ValueError("Cannot find any local profile for session %s" % sid)

    for prof in profiles:

        ftgt = '%s/%s' % (tgt_url, os.path.basename(prof))
        ret.append("%s/%s" % (tgt, os.path.basename(prof)))

        print "fetching '%s' to '%s'." % (prof, tgt_url)
        prof_file = saga.filesystem.File(prof, session=session)
        prof_file.copy(ftgt, flags=saga.filesystem.CREATE_PARENTS)
        prof_file.close()


    _, db, _, _, _ = ru.mongodb_connect (dburl)

    json_docs = get_session_docs(db, sid)

    pilots = json_docs['pilot']
    num_pilots = len(pilots)
    print "Session: %s" % sid
    print "Number of pilots in session: %d" % num_pilots

    for pilot in pilots:

        print "Processing pilot '%s'" % pilot['_id']

        sandbox  = saga.filesystem.Directory (pilot['sandbox'], session=session)
        profiles = sandbox.list('*.prof')

        for prof in profiles:

            ftgt = '%s/%s' % (tgt_url, prof)
            ret.append("%s/%s" % (tgt, prof))

            print "fetching '%s/%s' to '%s'." % (pilot['sandbox'], prof, tgt_url)
            prof_file = saga.filesystem.File("%s/%s" % (pilot['sandbox'], prof), session=session)
            prof_file.copy(ftgt, flags=saga.filesystem.CREATE_PARENTS)
            prof_file.close()

    return ret


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
            
            cores = 0

            if pilot['nodes'] and pilot['cores_per_node']:
                cores = len(pilot['nodes']) * pilot['cores_per_node']

            if started  : started  -= session_start
            if finished : finished -= session_start

            pilot_dict = {
                'sid'          : sid,
                'pid'          : pid, 
                'n_units'      : len(pilot.get ('unit_ids', list())), 
                'started'      : started,
                'finished'     : finished,
                'resource'     : description.get ('resource'),
                'cores'        : cores,
                'runtime'      : description.get ('runtime'),
                NEW            : None, 
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

            if not pilot_dict[NEW]:
                if pilot_dict[PENDING_LAUNCH]:
                    pilot_dict[NEW] = pilot_dict[PENDING_LAUNCH]
                else:
                    pilot_dict[NEW] = pilot_dict[LAUNCHING]

            pilot_dicts.append (pilot_dict)


        for unit in docs['unit']:

            uid         = unit['_id']
            started     = unit.get ('started')
            finished    = unit.get ('finished')
            description = unit.get ('description', dict())

            if started  : started  -= session_start
            if finished : finished -= session_start

            session_dict['n_units'] += 1

            unit_dict = {
                'sid'                  : sid, 
                'pid'                  : unit.get('pilot'), 
                'uid'                  : uid, 
                'started'              : started,
                'finished'             : finished,
                'cores'                : description.get ('cores'),
                'slots'                : unit.get ('slots'),
                NEW                    : None, 
                UNSCHEDULED            : None, 
                PENDING_INPUT_STAGING  : None, 
                STAGING_INPUT          : None, 
                PENDING_EXECUTION      : None, 
                SCHEDULING             : None, 
                ALLOCATING             : None, 
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

            # FIXME: there is more state messup afloat: some states are missing,
            # even though we know they have happened.  For one, we see data
            # being staged w/o having a record of InputStaging states.  Or we
            # find callback history entries for states which are not in the
            # history...
            #
            # We try to clean up to some extent.  The policy is like this, for
            # any [pending_state, state] pair:
            #
            # - if both are in the hist: great
            # - if one is in the hist, and the other in the cb hist, use like
            #   that, but ensure that pending_state <= state
            # - if both are in cb_hist, use them, apply same ordering assert.
            #   Use median if ordering is wrong
            # - if only on is in cb_host, use the same value for the other one
            # - if neither is anywhere, leave unset
            rec_hist = dict()
            cb_hist  = dict()

            for e in unit.get('statehistory', list()):
                state = e['state']
                timer = e['timestamp'] - session_start
                if state not in rec_hist:
                    rec_hist[state] = list()
                rec_hist[state].append(timer)

            for e in unit.get('callbackhistory', list()):
                state = e['state']
                timer = e['timestamp'] - session_start
                if state not in cb_hist:
                    cb_hist[state] = list()
                cb_hist[state].append(timer)

            statepairs = {STAGING_INPUT  : PENDING_INPUT_STAGING ,
                          STAGING_OUTPUT : PENDING_OUTPUT_STAGING}

            primary_states = [NEW                   ,
                              UNSCHEDULED           ,
                              STAGING_INPUT         ,
                              PENDING_EXECUTION     ,
                              SCHEDULING            ,
                              ALLOCATING            ,
                              EXECUTING             ,
                              STAGING_OUTPUT        ,
                              DONE                  ,
                              CANCELED              ,
                              FAILED                ]

            for state in primary_states:

                pend    = None
                t_state = None
                t_pend  = None

                ts_rec  = rec_hist.get (state) #         state time stamp from state hist
                ts_cb   = cb_hist.get  (state) #         state time stamp from cb    hist
                tp_rec  = None                 # pending state time stamp from state hist
                tp_cb   = None                 # pending state time stamp from cb    hist

                if  state in statepairs:
                    pend   = statepairs[state]
                    tp_rec = rec_hist.get (pend)
                    tp_cb  = cb_hist.get  (pend)

                # try to find a candidate for state timestamp
                if   ts_rec : t_state = ts_rec[0]
                elif ts_cb  : t_state = ts_cb [0]
                elif tp_rec : t_state = tp_rec[0]
                elif tp_cb  : t_state = tp_cb [0]

                # try to find a candidate for pending timestamp
                if   tp_rec : t_pend  = tp_rec[0]
                elif tp_cb  : t_pend  = tp_cb [0]

                # if there is no t_pend, check if there are two state times on
                # record (in the state hist), and if so, reorder
                if pend :
                    if t_state and not t_pend:
                        if ts_rec and len(ts_rec) == 2:
                            t_pend  = min (ts_rec)
                            t_state = max (ts_rec)
                        else:
                            t_pend  = t_state

                # make sure that any pending time comes before state time
                if pend:
                    if t_pend > t_state:
                      # print "%s : %s" % (uid, state)
                        t_med   = (t_pend + t_state) / 2
                        t_pend  = t_med
                        t_state = t_med

                # record the times for the data frame
                unit_dict[state] = t_state

                if pend :
                    unit_dict[pend] = t_pend


            if unit_dict[UNSCHEDULED] and unit_dict[SCHEDULING]:
                unit_dict[UNSCHEDULED] = min(unit_dict[UNSCHEDULED], unit_dict[SCHEDULING])

            if not unit_dict[NEW]:
                if unit_dict[UNSCHEDULED]:
                    unit_dict[NEW] = unit_dict[UNSCHEDULED]
                if unit_dict[SCHEDULING]:
                    unit_dict[NEW] = unit_dict[SCHEDULING]


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
def fetch_json (sid, dburl=None, tgt=None) :

    '''
    returns file name
    '''

    if not dburl:
        dburl = os.environ['RADICAL_PILOT_DBURL']

    if not dburl:
        raise RuntimeError ('Please set RADICAL_PILOT_DBURL')

    if not tgt:
        tgt = '.'

    _, db, _, _, _ = ru.mongodb_connect (dburl)

    json_docs = get_session_docs(db, sid)

    if tgt.startswith('/'):
        dst = '/%s/%s.json' % (tgt, sid)
    else:
        dst = '/%s/%s/%s.json' % (os.getcwd(), tgt, sid)

    ru.write_json (json_docs, dst)
    print "session written to %s" % dst

    return dst


