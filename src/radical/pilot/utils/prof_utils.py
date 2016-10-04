
import os
import csv
import copy
import glob
import time
import threading

import radical.utils as ru


# --------------------------------------------------------------------------
#
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
            
            for prof in glob.glob ("%s/%s-pilot.*.prof" % (datadir, sid)):
                print "     - %s" % prof
                frame = pd.read_csv(prof)
                exp_frames[exp].append ([frame, label])
                
    return exp_frames


# ------------------------------------------------------------------------------
#
def get_session_profile(sid):
    
    profdir = '%s/%s/' % (os.getcwd(), sid)

    if os.path.exists(profdir):
        # we have profiles locally
        profiles  = glob.glob("%s/*.prof"   % profdir)
        profiles += glob.glob("%s/*/*.prof" % profdir)
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

    assert(sid == json['session']['uid'])

    ret             = dict()
    ret['entities'] = dict()

    tree      = dict()
    tree[sid] = {'uid'   : sid,
                 'etype' : 'session',
                 'cfg'   : json['session']['cfg'],
                 'has'   : ['umgr', 'pmgr', 'pilot', 'unit'],
                 'umgr'  : list(),
                 'pmgr'  : list()
                }

    for pmgr in sorted(json['pmgr'], key=lambda k: k['uid']):
        uid = pmgr['uid']
        tree[sid]['pmgr'].append(uid)
        tree[uid] = {'uid'   : uid,
                     'etype' : 'pmgr',
                     'cfg'   : pmgr['cfg'],
                     'has'   : ['pilot'],
                     'pilot' : list()
                    }

    for umgr in sorted(json['umgr'], key=lambda k: k['uid']):
        uid = umgr['uid']
        tree[sid]['umgr'].append(uid)
        tree[uid] = {'uid'   : uid,
                     'etype' : 'umgr',
                     'cfg'   : umgr['cfg'],
                     'has'   : ['unit'],
                     'unit' : list()
                    }

    for pilot in sorted(json['pilot'], key=lambda k: k['uid']):
        uid  = pilot['uid']
        pmgr = pilot['pmgr']
        tree[pmgr]['pilot'].append(uid)
        tree[uid] = {'uid'   : uid,
                     'etype' : 'pilot',
                     'cfg'   : pilot['cfg'],
                     'has'   : ['unit'],
                     'unit' : list()
                    }

    for unit in sorted(json['unit'], key=lambda k: k['uid']):
        uid  = unit['uid']
        pid  = unit['umgr']
        umgr = unit['pilot']
        tree[pid ]['unit'].append(uid)
        tree[umgr]['unit'].append(uid)
        tree[uid] = {'uid'   : uid,
                     'etype' : 'unit',
                     'cfg'   : unit['description'],
                     'has'   : [],
                    }

    ret['tree'] = tree

    ret['entities']['pilot'] = {
            'state_model'  : rps._pilot_state_values,
            'state_values' : rps._pilot_state_inv_full,
            'event_model'  : dict(),
            }

    ret['entities']['unit'] = {
            'state_model'  : rps._unit_state_values,
            'state_values' : rps._unit_state_inv_full,
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

