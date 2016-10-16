
import os
import csv
import copy
import glob
import time
import threading

import radical.utils as ru


# ------------------------------------------------------------------------------
#
# when recombining profiles, we will get one NTP sync offset per profile, and
# thus potentially multiple such offsets per host.  If those differ more than
# a certain value (float, in seconds) from each other, we print a warning:
#
NTP_DIFF_WARN_LIMIT = 1.0


def prof2frame(prof):
    """
    expect a profile, ie. a list of profile rows which are dicts.  
    Write that profile to a temp csv and let pandas parse it into a frame.
    """

    import pandas as pd

    # create data frame from profile dicts
    frame = pd.DataFrame(prof)

    # --------------------------------------------------------------------------
    # add a flag to indicate entity type
    def _entity (row):
        if not row['uid']:
            return 'session'
        if 'unit' in row['uid']:
            return 'unit'
        if 'pilot' in row['uid']:
            return 'pilot'
        return 'session'
    frame['entity'] = frame.apply(lambda row: _entity (row), axis=1)

    # --------------------------------------------------------------------------
    # add a flag to indicate if a unit / pilot / ... is cloned
    def _cloned (row):
        if not row['uid']:
            return False
        else:
            return 'clone' in row['uid'].lower()
    frame['cloned'] = frame.apply(lambda row: _cloned (row), axis=1)

    return frame


# ------------------------------------------------------------------------------
#
def split_frame(frame):
    """
    expect a profile frame, and split it into separate frames for:
      - session
      - pilots
      - units
    """

    session_frame = frame[frame['entity'] == 'session']
    pilot_frame   = frame[frame['entity'] == 'pilot']
    unit_frame    = frame[frame['entity'] == 'unit']

    return session_frame, pilot_frame, unit_frame


# ------------------------------------------------------------------------------
#
def get_experiment_frames(experiments, datadir=None):
    """
    read profiles for all sessions in the given 'experiments' dict.  That dict
    is expected to be like this:

    { 'test 1' : [ [ 'rp.session.thinkie.merzky.016609.0007',         'stampede popen sleep 1/1/1/1 (?)'] ],
      'test 2' : [ [ 'rp.session.ip-10-184-31-85.merzky.016610.0112', 'stampede shell sleep 16/8/8/4'   ] ],
      'test 3' : [ [ 'rp.session.ip-10-184-31-85.merzky.016611.0013', 'stampede shell mdrun 16/8/8/4'   ] ],
      'test 4' : [ [ 'rp.session.titan-ext4.marksant1.016607.0005',   'titan    shell sleep 1/1/1/1 a'  ] ],
      'test 5' : [ [ 'rp.session.titan-ext4.marksant1.016607.0006',   'titan    shell sleep 1/1/1/1 b'  ] ],
      'test 6' : [ [ 'rp.session.ip-10-184-31-85.merzky.016611.0013', 'stampede - isolated',            ],
                   [ 'rp.session.ip-10-184-31-85.merzky.016612.0012', 'stampede - integrated',          ],
                   [ 'rp.session.titan-ext4.marksant1.016607.0006',   'blue waters - integrated'        ] ]
    }  name in 

    ie. iname in t is a list of experiment names, and each label has a list of
    session/label pairs, where the label will be later used to label (duh) plots.

    we return a similar dict where the session IDs are data frames
    """
    import pandas as pd

    exp_frames  = dict()

    if not datadir:
        datadir = os.getcwd()

    print 'reading profiles in %s' % datadir

    for exp in experiments:
        print " - %s" % exp
        exp_frames[exp] = list()

        for sid, label in experiments[exp]:
            print "   - %s" % sid
            
            import glob
            for prof in glob.glob ("%s/%s-pilot.*.prof" % (datadir, sid)):
                print "     - %s" % prof
                frame = pd.read_csv(prof)
                exp_frames[exp].append ([frame, label])
                
    return exp_frames


# ------------------------------------------------------------------------------
#
def drop_units(cfg, units, name, mode, drop_cb=None, prof=None, logger=None):
    """
    For each unit in units, check if the queue is configured to drop
    units in the given mode ('in' or 'out').  If drop is set to 0, the units
    list is returned as is.  If drop is set to one, all cloned units are
    removed from the list.  If drop is set to two, an empty list is returned.

    For each dropped unit, we check if 'drop_cb' is defined, and call
    that callback if that is the case, with the signature:

      drop_cb(unit=unit, name=name, mode=mode, prof=prof, logger=logger)
    """

    # blowup is only enabled on profiling
    if 'RADICAL_PILOT_PROFILE' not in os.environ:
        if logger:
            logger.debug('no profiling - no dropping')
        return units

    if not units:
      # if logger:
      #     logger.debug('no units - no dropping')
        return units

    drop = cfg.get('drop', {}).get(name, {}).get(mode, 1)

    if drop == 0:
      # if logger:
      #     logger.debug('dropped nothing')
        return units

    return_list = True
    if not isinstance(units, list):
        return_list = False
        units = [units]

    if drop == 2:
        if drop_cb:
            for unit in units:
                drop_cb(unit=unit, name=name, mode=mode, prof=prof, logger=logger)
        if logger:
            logger.debug('dropped all')
            for unit in units:
                logger.debug('dropped %s', unit['_id'])
        if return_list: return []
        else          : return None

    if drop != 1:
        raise ValueError('drop[%s][%s] not in [0, 1, 2], but is %s' \
                      % (name, mode, drop))

    ret = list()
    for unit in units :
        if '.clone_' not in unit['_id']:
            ret.append(unit)
          # if logger:
          #     logger.debug('dropped not %s', unit['_id'])
        else:
            if drop_cb:
                drop_cb(unit=unit, name=name, mode=mode, prof=prof, logger=logger)
            if logger:
                logger.debug('dropped %s', unit['_id'])

    if return_list: 
        return ret
    else: 
        if ret: return ret[0]
        else  : return None


# ------------------------------------------------------------------------------
#
def clone_units(cfg, units, name, mode, prof=None, clone_cb=None, logger=None):
    """
    For each unit in units, add 'factor' clones just like it, just with
    a different ID (<id>.clone_001).  The factor depends on the context of
    this clone call (ie. the queue name), and on mode (which is 'input' or
    'output').  This methid will always return a list.

    For each cloned unit, we check if 'clone_cb' is defined, and call
    that callback if that is the case, with the signature:

      clone_cb(unit=unit, name=name, mode=mode, prof=prof, logger=logger)
    """

    if units == None:
        if logger:
            logger.debug('no units - no cloning')
        return list()

    if not isinstance(units, list):
        units = [units]

    # blowup is only enabled on profiling
    if 'RADICAL_PILOT_PROFILE' not in os.environ:
        if logger:
            logger.debug('no profiling - no cloning')
        return units

    if not units:
        # nothing to clone...
        if logger:
            logger.debug('No units - no cloning')
        return units

    factor = cfg.get('clone', {}).get(name, {}).get(mode, 1)

    if factor == 1:
        if logger:
            logger.debug('cloning with factor [%s][%s]: 1' % (name, mode))
        return units

    if factor < 1:
        raise ValueError('clone factor must be >= 1 (not %s)' % factor)

    ret = list()
    for unit in units :

        uid = unit['_id']

        for idx in range(factor-1) :

            clone    = copy.deepcopy(dict(unit))
            clone_id = '%s.clone_%05d' % (uid, idx+1)

            for key in clone :
                if isinstance (clone[key], basestring) :
                    clone[key] = clone[key].replace (uid, clone_id)

            idx += 1
            ret.append(clone)

            if clone_cb:
                clone_cb(unit=clone, name=name, mode=mode, prof=prof, logger=logger)

        # Append the original cu last, to increase the likelyhood that
        # application state only advances once all clone states have also
        # advanced (they'll get pushed onto queues earlier).  This cannot be
        # relied upon, obviously.
        ret.append(unit)

    if logger:
        logger.debug('cloning with factor [%s][%s]: %s gives %s units',
                     name, mode, factor, len(ret))

    return ret


# ------------------------------------------------------------------------------
#
def read_profiles(profiles):
    """
    We read all profiles as CSV files and parse them.  For each profile,
    we back-calculate global time (epoch) from the synch timestamps.  
    """

    ret    = dict()
    fields = ru.Profiler.fields

    for prof in profiles:
        rows = list()
        with open(prof, 'r') as csvfile:
            reader = csv.DictReader(csvfile, fieldnames=fields)
            for row in reader:

                # skip header
                if row['time'].startswith('#'):
                    continue

                row['time'] = float(row['time'])
    
                # store row in profile
                rows.append(row)
    
        ret[prof] = rows

    return ret


# ------------------------------------------------------------------------------
#
def combine_profiles(profs):
    """
    We merge all profiles and sorted by time.

    This routine expectes all profiles to have a synchronization time stamp.
    Two kinds of sync timestamps are supported: absolute and relative.  

    Time syncing is done based on 'sync abs' timestamps, which we expect one to
    be available per host (the first profile entry will contain host
    information).  All timestamps from the same host will be corrected by the
    respectively determined ntp offset.
    """

    pd_rel = dict() # profiles which have relative time refs

    t_host = dict() # time offset per host
    p_glob = list() # global profile
    t_min  = None   # absolute starting point of prof session
    c_qed  = 0      # counter for profile closing tag

    for pname, prof in profs.iteritems():

        if not len(prof):
            print 'empty profile %s' % pname
            continue

        if not prof[0]['msg']:
            print 'unsynced profile %s' % pname
            continue

        t_prof = prof[0]['time']

        host, ip, t_sys, t_ntp, t_mode = prof[0]['msg'].split(':')
        host_id = '%s:%s' % (host, ip)

        if t_min:
            t_min = min(t_min, t_prof)
        else:
            t_min = t_prof

        if t_mode != 'sys':
            continue

        # determine the correction for the given host
        t_sys = float(t_sys)
        t_ntp = float(t_ntp)
        t_off = t_sys - t_ntp

        if host_id in t_host:
<<<<<<< HEAD
          # print 'conflicting time sync for %s (%s)' % (pname, host_id)
            continue
=======

            accuracy = max(accuracy, t_off-t_host[host_id])

            if abs(t_off-t_host[host_id]) > NTP_DIFF_WARN_LIMIT:
                print 'conflict sync   %-35s (%-35s) %6.1f : %6.1f :  %12.5f' \
                        % (os.path.basename(pname), host_id, t_off, t_host[host_id], (t_off-t_host[host_id]))

            continue # we always use the first match

      # print 'store time sync %-35s (%-35s) %6.1f' \
      #         % (os.path.basename(pname), host_id, t_off)
>>>>>>> 1999dfb... some documentation on NTP diff warnings, higher limit

        t_host[host_id] = t_off

    # FIXME: this should be removed once #1117 is fixed
    for pname, prof in profs.iteritems():
        i=0
        l=len(prof)
        while i<l:
            if prof[i]['time'] == 1.0:
                if i < l-1:
                    prof[i]['time'] = prof[i+1]['time']
                elif i > 0:
                    prof[i]['time'] = prof[i-1]['time']
                else:
                    prof[i]['time'] = t_0
            i += 1

    unsynced = set()
    for pname, prof in profs.iteritems():

        if not len(prof):
            continue

        if not prof[0]['msg']:
            continue

        host, ip, _, _, _ = prof[0]['msg'].split(':')
        host_id = '%s:%s' % (host, ip)
        if host_id in t_host:
            t_off   = t_host[host_id]
        else:
            unsynced.add(host_id)
            t_off = 0.0

        t_0 = prof[0]['time']
        t_0 -= t_min

        # correct profile timestamps
        for row in prof:

            t_orig = row['time'] 

            row['time'] -= t_min
            row['time'] -= t_off

            # count closing entries
            if row['event'] == 'QED':
                c_qed += 1

        # add profile to global one
        p_glob += prof


      # # Check for proper closure of profiling files
      # if c_qed == 0:
      #     print 'WARNING: profile "%s" not correctly closed.' % prof
      # if c_qed > 1:
      #     print 'WARNING: profile "%s" closed %d times.' % (prof, c_qed)

    # sort by time and return
    p_glob = sorted(p_glob[:], key=lambda k: k['time']) 

    if unsynced:
        print 'unsynced hosts: %s' % list(unsynced)

    return p_glob


# ------------------------------------------------------------------------------
# 
def clean_profile(profile, sid):
    """
    This method will prepare a profile for consumption in radical.analytics.  It
    performs the following actions:

      - makes sure all events have a `ename` entry
      - remove all state transitions to `CANCELLED` if a different final state 
        is encountered for the same uid
      - assignes the session uid to all events without uid
      - makes sure that state transitions have an `ename` set to `state`
    """

    from radical.pilot import states as rps

    entities = dict()  # things which have a uid

    for event in profile:

        uid   = event['uid']
        state = event['state']
        time  = event['time']
        name  = event['event']

        del(event['event'])

        # we derive entity_type from the uid -- but funnel 
        # some cases into the session
        if uid:
            event['entity_type'] = uid.split('.',1)[0]

        elif uid == 'root':
            event['entity_type'] = 'session'
            event['uid']         = sid
            uid = sid

        else:
            event['entity_type'] = 'session'
            event['uid']         = sid
            uid = sid

        if uid not in entities:
            entities[uid] = dict()
            entities[uid]['states'] = dict()
            entities[uid]['events'] = list()

        if name == 'advance':

            # this is a state progression
            assert(state)
            assert(uid)

            event['event_type'] = 'state'
            skip = False

            if state in rps.FINAL:

                # a final state will cancel any previoud CANCELED state
                if rps.CANCELED in entities[uid]['states']:
                    del (entities[uid]['states'][rps.CANCELED])

                # vice-versa, we will not add CANCELED if a final
                # state already exists:
                if state == rps.CANCELED:
                    if any([s in entities[uid]['states'] 
                        for s in rps.FINAL]):
                        skip = True
                        continue

            if state in entities[uid]['states']:
                # ignore duplicated recordings of state transitions
                skip = True
                continue
              # raise ValueError('double state (%s) for %s' % (state, uid))

            if not skip:
                entities[uid]['states'][state] = event

        else:
            # FIXME: define different event types (we have that somewhere)
            event['event_type'] = 'event'
            entities[uid]['events'].append(event)


    # we have evaluated, cleaned and sorted all events -- now we recreate
    # a clean profile out of them
    ret = list()
    for uid,entity in entities.iteritems():

        ret += entity['events']
        for state,event in entity['states'].iteritems():
            ret.append(event)

    # sort by time and return
    ret = sorted(ret[:], key=lambda k: k['time']) 

    return ret


# ------------------------------------------------------------------------------
#
def get_session_profile(sid, src=None):
    
    if not src:
        src = "%s/%s" % (os.getcwd(), sid)

    if os.path.exists(src):
        # we have profiles locally
        profiles  = glob.glob("%s/*.prof"   % src)
        profiles += glob.glob("%s/*/*.prof" % src)
    else:
        # need to fetch profiles
        from .session import fetch_profiles
        profiles = fetch_profiles(sid=sid, skip_existing=True)

    profs = read_profiles(profiles)
    prof  = combine_profiles(profs)
    prof  = clean_profile(prof, sid)

    return prof


# ------------------------------------------------------------------------------
# 
def get_session_description(sid, src=None, dburl=None):
    """
    This will return a description which is usable for radical.analytics
    evaluation.  It informs about
      - set of stateful entities
      - state models of those entities
      - event models of those entities (maybe)
      - configuration of the application / module

    If `src` is given, it is interpreted as path to search for session
    information (json dump).  `src` defaults to `$PWD/$sid`.

    if `dburl` is given, its value is used to fetch session information from
    a database.  The dburl value defaults to `RADICAL_PILOT_DBURL`.
    """

    from radical.pilot import states as rps
    from .session      import fetch_json

    if not src:
        src = "%s/%s" % (os.getcwd(), sid)

    ftmp = fetch_json(sid=sid, dburl=dburl, tgt=src, skip_existing=True)
    json = ru.read_json(ftmp)


    # make sure we have uids
    def fix_json(json):
        def fix_uids(json):
            if isinstance(json, list):
                for elem in json:
                    fix_uids(elem)
            elif isinstance(json, dict):
                if 'unitmanager' in json and 'umgr' not in json:
                    json['umgr'] = json['unitmanager']
                if 'pilotmanager' in json and 'pmgr' not in json:
                    json['pmgr'] = json['pilotmanager']
                if '_id' in json and 'uid' not in json:
                    json['uid'] = json['_id']
                    if not 'cfg' in json:
                        json['cfg'] = dict()
                for k,v in json.iteritems():
                    fix_uids(v)
        fix_uids(json)
    fix_json(json)

    ru.write_json(json, '/tmp/t.json')

    assert(sid == json['session']['uid'])

    ret             = dict()
    ret['entities'] = dict()

    tree      = dict()
    tree[sid] = {'uid'      : sid,
                 'etype'    : 'session',
               # 'cfg'      : json['session']['cfg'],
                 'has'      : ['umgr', 'pmgr'],
                 'children' : list()
                }

    for pmgr in sorted(json['pmgr'], key=lambda k: k['uid']):
        uid = pmgr['uid']
        tree[sid]['children'].append(uid)
        tree[uid] = {'uid'      : uid,
                     'etype'    : 'pmgr',
                   # 'cfg'      : pmgr['cfg'],
                     'has'      : ['pilot'],
                     'children' : list()
                    }

    for umgr in sorted(json['umgr'], key=lambda k: k['uid']):
        uid = umgr['uid']
        tree[sid]['children'].append(uid)
        tree[uid] = {'uid'      : uid,
                     'etype'    : 'umgr',
               #     'cfg'      : umgr['cfg'],
                     'has'      : ['unit'],
                     'children' : list()
                    }

    for pilot in sorted(json['pilot'], key=lambda k: k['uid']):
        uid  = pilot['uid']
        pmgr = pilot['pmgr']
        tree[pmgr]['children'].append(uid)
        tree[uid] = {'uid'      : uid,
                     'etype'    : 'pilot',
               #     'cfg'      : pilot['cfg'],
                     'has'      : ['unit'],
                     'children' : list()
                    }

    for unit in sorted(json['unit'], key=lambda k: k['uid']):
        uid  = unit['uid']
        pid  = unit['umgr']
        umgr = unit['pilot']
        tree[pid ]['children'].append(uid)
        tree[umgr]['children'].append(uid)
        tree[uid] = {'uid'      : uid,
                     'etype'    : 'unit',
               #     'cfg'      : unit['description'],
                     'has'      : list(),
                     'children' : list()
                    }

    ret['tree'] = tree

    import pprint, sys
    pprint.pprint(tree)

    ret['entities']['pilot'] = {
            'state_model'  : rps.pilot_state_by_value,
            'state_values' : rps.pilot_state_value,
            'event_model'  : dict(),
            }

    ret['entities']['unit'] = {
            'state_model'  : rps.unit_state_by_value,
            'state_values' : rps.unit_state_value,
            'event_model'  : dict(),
            }

    ret['entities']['session'] = {
            'state_model'  : None, # session has no states, only events
            'state_values' : None,
            'event_model'  : dict(),
            }

    ret['config'] = dict() # magic to get session config goes here

    return ret


# ------------------------------------------------------------------------------

