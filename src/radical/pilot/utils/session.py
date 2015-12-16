import os
import sys
import glob
import saga
import tarfile

import radical.utils as ru
from   radical.pilot.states import *
from . import version_detail as rp_version_detail
from . import logger

from db_utils import *


# ------------------------------------------------------------------------------
#
def fetch_profiles (sid, dburl=None, client=None, tgt=None, access=None, 
        session=None, skip_existing=False):
    '''
    sid: session for which all profiles are fetched
    client: dir to look for client session profiles
    tgt: dir to store the profile in

    returns list of file names
    '''

    ret = list()

    if not dburl:
        dburl = os.environ['RADICAL_PILOT_DBURL']

    if not dburl:
        raise RuntimeError ('Please set RADICAL_PILOT_DBURL')

    if not client:
        client = os.getcwd()
            
    if not tgt:
        tgt = os.getcwd()
            
    if not tgt.startswith('/') and '://' not in tgt:
        tgt = "%s/%s" % (os.getcwd(), tgt)

    # we always create a session dir as real target
    tgt_url = saga.Url("%s/%s/" % (tgt, sid))

    # Turn URLs without schema://host into file://localhost,
    # so that they dont become interpreted as relative.
    if not tgt_url.schema:
        tgt_url.schema = 'file'
    if not tgt_url.host:
        tgt_url.host = 'localhost'

    # first fetch session profile
    # FIXME: should we record pwd or profile location in db session?  Or create
    #        a sandbox like dir for storing profiles and logs?
    client_profile = "%s/%s.prof" % (client, sid)

    ftgt = saga.Url('%s/%s' % (tgt_url, os.path.basename(client_profile)))
    ret.append("%s" % ftgt.path)

    if skip_existing and os.path.isfile(ftgt.path) \
            and os.stat(ftgt.path).st_size > 0:

        logger.report.info("\t- %s\n" % client_profile.split('/')[-1])

    else:

        logger.report.info("\t+ %s\n" % client_profile.split('/')[-1])
        prof_file = saga.filesystem.File(client_profile, session=session)
        prof_file.copy(ftgt, flags=saga.filesystem.CREATE_PARENTS)
        prof_file.close()

    _, db, _, _, _ = ru.mongodb_connect (dburl)

    json_docs = get_session_docs(db, sid)

    pilots = json_docs['pilot']
    num_pilots = len(pilots)
 #  print "Session: %s" % sid
 #  print "Number of pilots in session: %d" % num_pilots

    for pilot in pilots:

      # print "Processing pilot '%s'" % pilot['_id']

        sandbox_url = saga.Url(pilot['sandbox'])

        if access:
            # Allow to use a different access scheme than used for the the run.
            # Useful if you ran from the headnode, but would like to retrieve
            # the profiles to your desktop (Hello Titan).
            access_url = saga.Url(access)
            sandbox_url.schema = access_url.schema
            sandbox_url.host = access_url.host

          # print "Overriding remote sandbox: %s" % sandbox_url

        sandbox  = saga.filesystem.Directory (sandbox_url, session=session)

        # Try to fetch a tarball of profiles, so that we can get them all in one (SAGA) go!
        PROFILES_TARBALL = '%s.prof.tgz' % pilot['_id']
        tarball_available = False
        try:
            if sandbox.is_file(PROFILES_TARBALL):
                print "Profiles tarball exists!"

                ftgt = saga.Url('%s/%s' % (tgt_url, PROFILES_TARBALL))

                if skip_existing and os.path.isfile(ftgt.path) \
                        and os.stat(ftgt.path).st_size > 0:

                    print "Skipping fetching of '%s/%s' to '%s'." % (sandbox_url, PROFILES_TARBALL, tgt_url)
                    tarball_available = True
                else:

                    print "Fetching '%s%s' to '%s'." % (sandbox_url, PROFILES_TARBALL, tgt_url)
                    prof_file = saga.filesystem.File("%s%s" % (sandbox_url, PROFILES_TARBALL), session=session)
                    prof_file.copy(ftgt, flags=saga.filesystem.CREATE_PARENTS)
                    prof_file.close()

                    tarball_available = True
            else:
                print "Profiles tarball doesnt exists!"

        except saga.DoesNotExist:
            print "exception(TODO): profiles tarball doesnt exists!"

        try:
            os.mkdir("%s/%s" % (tgt_url.path, pilot['_id']))
        except OSError:
            pass

        # We now have a local tarball
        if tarball_available:
            print "Extracting tarball %s into '%s'." % (ftgt.path, tgt_url.path)
            tarball = tarfile.open(ftgt.path)
            tarball.extractall("%s/%s" % (tgt_url.path, pilot['_id']))

            profiles = glob.glob("%s/%s/*.prof" % (tgt_url.path, pilot['_id']))
            print "Tarball %s extracted to '%s/%s/'." % (ftgt.path, tgt_url.path, pilot['_id'])
            ret.extend(profiles)

            # If extract succeeded, no need to fetch individual profiles
            continue

        # If we dont have a tarball (for whichever reason), fetch individual profiles
        profiles = sandbox.list('*.prof')

        for prof in profiles:

            ftgt = saga.Url('%s/%s/%s' % (tgt_url, pilot['_id'], prof))
            ret.append("%s" % ftgt.path)

            if skip_existing and os.path.isfile(ftgt.path) \
                             and os.stat(ftgt.path).st_size > 0:

                logger.report.info("\t- %s\n" % str(prof).split('/')[-1])
                continue

            logger.report.info("\t+ %s\n" % str(prof).split('/')[-1])
            prof_file = saga.filesystem.File("%s%s" % (sandbox_url, prof), session=session)
            prof_file.copy(ftgt, flags=saga.filesystem.CREATE_PARENTS)
            prof_file.close()

    return ret


# ------------------------------------------------------------------------------
#
def get_session_frames (sids, db=None, cachedir=None) :

    # use like this: 
    #
    # session_frame, pilot_frame, unit_frame = rpu.get_session_frames (session, db, cachedir)
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

    mongo = None

    if not db:
        dburl = os.environ.get('RADICAL_PILOT_DBURL')
        if not dburl:
            raise RuntimeError ('Please set RADICAL_PILOT_DBURL')

        mongo, db, _, _, _ = ru.mongodb_connect(dburl)


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
            else:
                cores = description.get('cores')

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
                EXECUTING_PENDING      : None,
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
                              EXECUTING_PENDING     ,
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

    if mongo:
        mongo.close()

    return session_frame, pilot_frame, unit_frame



# ------------------------------------------------------------------------------
#
def fetch_json(sid, dburl=None, tgt=None, skip_existing=False):

    '''
    returns file name
    '''

    if not tgt:
        tgt = '.'

    if tgt.startswith('/'):
        # Assume an absolute path
        dst = os.path.join(tgt, '%s.json' % sid)
    else:
        # Assume a relative path
        dst = os.path.join(os.getcwd(), tgt, '%s.json' % sid)

    if skip_existing and os.path.isfile(dst) \
            and os.stat(dst).st_size > 0:

        print "session already in %s" % dst

    else:

        if not dburl:
            dburl = os.environ['RADICAL_PILOT_DBURL']

        if not dburl:
            raise RuntimeError ('Please set RADICAL_PILOT_DBURL')

        mongo, db, _, _, _ = ru.mongodb_connect(dburl)

        json_docs = get_session_docs(db, sid)
        ru.write_json(json_docs, dst)

        print "session written to %s" % dst

        mongo.close()

    return dst


#--------------------------------------------------------------------------
#
# Insert (experiment) metadata into an active session
# RP stack version info always get added.
#
def inject_metadata(session, metadata):

    if not isinstance(metadata, dict):
        raise Exception("Session metadata should be a dict!")

    if session is None:
        raise Exception("No session specified.")

    # Always record the radical software stack
    metadata['radical_stack'] = {
        'rp': rp_version_detail,
        'rs': saga.version_detail,
        'ru': ru.version_detail
    }

    result = session._dbs._s.update(
        {"_id": session._uid},
        {"$set" : {"metadata": metadata}}
    )
