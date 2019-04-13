
import os
import glob

import radical.utils               as ru
from   radical.pilot import states as rps


# ------------------------------------------------------------------------------
#
def get_hostmap(profile):
    '''
    We abuse the profile combination to also derive a pilot-host map, which
    will tell us on what exact host each pilot has been running.  To do so, we
    check for the PMGR_ACTIVE advance event in agent_0.prof, and use the NTP
    sync info to associate a hostname.
    '''
    # FIXME: This should be replaced by proper hostname logging
    #        in `pilot.resource_details`.

    hostmap = dict()  # map pilot IDs to host names
    for entry in profile:
        if  entry[ru.EVENT] == 'hostname':
            hostmap[entry[ru.UID]] = entry[ru.MSG]

    return hostmap


# ------------------------------------------------------------------------------
#
def get_hostmap_deprecated(profiles):
    '''
    This method mangles combine_profiles and get_hostmap, and is deprecated.  At
    this point it only returns the hostmap
    '''

    hostmap = dict()  # map pilot IDs to host names
    for pname, prof in profiles.iteritems():

        if not len(prof):
            continue

        if not prof[0][ru.MSG]:
            continue

        host, ip, _, _, _ = prof[0][ru.MSG].split(':')
        host_id = '%s:%s' % (host, ip)

        for row in prof:

            if 'agent_0.prof' in pname    and \
                row[ru.EVENT] == 'advance' and \
                row[ru.STATE] == rps.PMGR_ACTIVE:
                hostmap[row[ru.UID]] = host_id
                break

    return hostmap


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

    #  filter out some frequent, but uninteresting events
    efilter = {ru.EVENT : ['publish', 'work start', 'work done'], 
               ru.MSG   : ['update unit state', 'unit update pushed', 
                            'bulked', 'bulk size']
              }

    profiles          = ru.read_profiles(profiles, sid, efilter=efilter)
    profile, accuracy = ru.combine_profiles(profiles)
    profile           = ru.clean_profile(profile, sid, rps.FINAL, rps.CANCELED)
    hostmap           = get_hostmap(profile)

    if not hostmap:
        # FIXME: legacy host notation - deprecated
        hostmap = get_hostmap_deprecated(profiles)

    return profile, accuracy, hostmap


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

    if os.path.isfile('%s/%s.json' % (src, sid)):
        json = ru.read_json('%s/%s.json' % (src, sid))
    else:
        ftmp = fetch_json(sid=sid, dburl=dburl, tgt=src, skip_existing=True)
        json = ru.read_json(ftmp)

    # make sure we have uids
    # FIXME v0.47: deprecate
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
                    if 'cfg' not in json:
                        json['cfg'] = dict()
                for k,v in json.iteritems():
                    fix_uids(v)
        fix_uids(json)
    fix_json(json)

    assert(sid == json['session']['uid']), 'sid inconsistent'

    ret             = dict()
    ret['entities'] = dict()

    tree      = dict()
    tree[sid] = {'uid'      : sid,
                 'etype'    : 'session',
                 'cfg'      : json['session']['cfg'],
                 'has'      : ['umgr', 'pmgr'],
                 'children' : list()
                }

    for pmgr in sorted(json['pmgr'], key=lambda k: k['uid']):
        uid = pmgr['uid']
        tree[sid]['children'].append(uid)
        tree[uid] = {'uid'      : uid,
                     'etype'    : 'pmgr',
                     'cfg'      : pmgr['cfg'],
                     'has'      : ['pilot'],
                     'children' : list()
                    }

    for umgr in sorted(json['umgr'], key=lambda k: k['uid']):
        uid = umgr['uid']
        tree[sid]['children'].append(uid)
        tree[uid] = {'uid'      : uid,
                     'etype'    : 'umgr',
                     'cfg'      : umgr['cfg'],
                     'has'      : ['unit'],
                     'children' : list()
                    }
        # also inject the pilot description, and resource specifically
        tree[uid]['description'] = dict()

    for pilot in sorted(json['pilot'], key=lambda k: k['uid']):
        uid  = pilot['uid']
        pmgr = pilot['pmgr']
        tree[pmgr]['children'].append(uid)
        tree[uid] = {'uid'        : uid,
                     'etype'      : 'pilot',
                     'cfg'        : pilot['cfg'],
                     'description': pilot['description'],
                     'has'        : ['unit'],
                     'children'   : list()
                    }
        # also inject the pilot description, and resource specifically

    for unit in sorted(json['unit'], key=lambda k: k['uid']):
        uid  = unit['uid']
        pid  = unit['umgr']
        umgr = unit['pilot']
        tree[pid ]['children'].append(uid)
        tree[umgr]['children'].append(uid)
        tree[uid] = {'uid'         : uid,
                     'etype'       : 'unit',
                     'cfg'         : unit,
                     'description' : unit['description'],
                     'has'         : list(),
                     'children'    : list()
                    }
        # remove duplicate
        del(tree[uid]['cfg']['description'])

    ret['tree'] = tree

    ret['entities']['pilot']   = {'state_model'  : rps._pilot_state_values,
                                  'state_values' : rps._pilot_state_inv_full,
                                  'event_model'  : dict()}
    ret['entities']['unit']    = {'state_model'  : rps._unit_state_values,
                                  'state_values' : rps._unit_state_inv_full,
                                  'event_model'  : dict()}
    ret['entities']['session'] = {'state_model'  : None,  # has no states
                                  'state_values' : None,
                                  'event_model'  : dict()}

    ret['config'] = dict()  # session config goes here

    return ret


# ------------------------------------------------------------------------------

