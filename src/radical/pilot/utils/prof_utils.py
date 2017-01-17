
import os
import csv
import copy
import glob
import time
import threading

import radical.utils  as ru

from .. import states as rps


NTP_DIFF_WARN_LIMIT = 1.0

# ------------------------------------------------------------------------------
#
_prof_fields  = ['time', 'name', 'uid', 'state', 'event', 'msg']


# ------------------------------------------------------------------------------
#
# profile class
#
class Profiler (object):
    """
    This class is really just a persistent file handle with a convenient way
    (prof()) of writing lines with timestamp and events to that file.  Any
    profiling intelligence is applied when reading and evaluating the created
    profiles.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, name, path=None):

        # this init is only called once (globally).  We synchronize clocks and
        # set timestamp_zero

        self._handles = dict()

        # we only profile if so instructed
        if 'RADICAL_PILOT_PROFILE' in os.environ:
            self._enabled = True
        else:
            self._enabled = False
            return

        self._ts_zero, self._ts_abs, self._ts_mode = self._timestamp_init()

        if not path:
            path = os.getcwd()

        self._path = path
        self._name = name

        try:
            os.makedirs(self._path)
        except OSError:
            pass # already exists

        self._handle = open("%s/%s.prof" % (self._path, self._name), 'a')

        # write header and time normalization info
        # NOTE: Don't forget to sync any format changes in the bootstrapper
        #       and downstream analysis tools too!
        self._handle.write("#%s\n" % (','.join(_prof_fields)))
        self._handle.write("%.4f,%s:%s,%s,%s,%s,%s\n" % \
                           (self.timestamp(), self._name, "", "", "", 'sync abs',
                            "%s:%s:%s:%s:%s" % (
                                ru.get_hostname(), ru.get_hostip(),
                                self._ts_zero, self._ts_abs, self._ts_mode)))


    # ------------------------------------------------------------------------------
    #
    @property
    def enabled(self):

        return self._enabled


    # ------------------------------------------------------------------------------
    #
    def close(self):

        if self._enabled:
            self.prof("QED")
            self._handle.close()


    # ------------------------------------------------------------------------------
    #
    def flush(self):

        if self._enabled:
            self.prof("flush")
            self._handle.flush()
            # https://docs.python.org/2/library/stdtypes.html?highlight=file%20flush#file.flush
            os.fsync(self._handle.fileno())


    # ------------------------------------------------------------------------------
    #
    def prof(self, event, uid=None, state=None, msg=None, timestamp=None, logger=None):

        if not self._enabled:
            return

        if logger:
            logger("%s (%10s%s) : %s", event, uid, state, msg)

        if not timestamp:
            timestamp = self.timestamp()

        tid = threading.current_thread().name

        if not uid  : uid   = ''
        if not msg  : msg   = ''
        if not state: state = ''


        # NOTE: Don't forget to sync any format changes in the bootstrapper
        #       and downstream analysis tools too!
        self._handle.write("%.4f,%s:%s,%s,%s,%s,%s\n" \
                % (timestamp, self._name, tid, uid, state, event, msg))


    # --------------------------------------------------------------------------
    #
    def _timestamp_init(self):
        """
        return a tuple of [system time, absolute time]
        """

        # retrieve absolute timestamp from an external source
        #
        # We first try to contact a network time service for a timestamp, if that
        # fails we use the current system time.
        try:
            import ntplib

            ntphost = os.environ.get('RADICAL_PILOT_NTPHOST', '0.pool.ntp.org')

            t_one = time.time()
            response = ntplib.NTPClient().request(ntphost, timeout=1)
            t_two = time.time()

            ts_ntp = response.tx_time
            ts_sys = (t_one + t_two) / 2.0
            return [ts_sys, ts_ntp, 'ntp']

        except Exception:
            pass

        t = time.time()
        return [t,t, 'sys']


    # --------------------------------------------------------------------------
    #
    def timestamp(self):

        return time.time()


# --------------------------------------------------------------------------
#
def timestamp():
    # human readable absolute UTC timestamp for log entries in database
    return time.time()


# ------------------------------------------------------------------------------
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
def read_profiles(profiles):
    """
    We read all profiles as CSV files and parse them.  For each profile,
    we back-calculate global time (epoch) from the synch timestamps.

    """
    ret = dict()

    for prof in profiles:
        p     = list()
        with open(prof, 'r') as csvfile:
            reader = csv.DictReader(csvfile, fieldnames=_prof_fields)
            for row in reader:

                # skip header
                if row['time'].startswith('#'):
                    continue

                row['time'] = float(row['time'])

                # store row in profile
                p.append(row)

        ret[prof] = p

    return ret


# ------------------------------------------------------------------------------
#
def combine_profiles(profs, sid):
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
    t_smin = None   # absolute starting point of prof session
    c_qed  = 0      # counter for profile closing tag

    accuracy     = 0.0    # approx. accuracy of time stamps
    hostmap      = dict() # map uid to host
    session_host = ''     # hostid of main session profile

    # we have relative profiles, where the entry timestamps are relative to
    # the profile start, and thus relative to the sync timestamp; and we
    # have absolute profiles which record timestamps in seconds since epoch.
    # The reltive ones are actually deprecated - but we allow for those for
    # backward compatibility.  Wehn we find a profile where the *last*
    # timestamp is smaller than a year-second (60*60*24*365 seconds), we
    # assume it is a relative profile, and we add the sync time to all
    # entries.
    year_second = 60*60*24*365
    for pname, prof in profs.iteritems():

        if not prof:
            continue

        last_entry = prof[-1]
        if last_entry['time'] >= year_second:
            # absolite profile, nothing to do
            continue

        # get sync timstamp from prof[0]
        if not prof[0]['msg']:
            # unsynced profile - nothing we can do
            continue

        elems = prof[0]['msg'].split(':')
        if len(elems) == 5:
            _, _, _, time_sync, _ = elems
        elif len(elems) == 4:
            t_sync, _, _, _ = elems
        else:
            # cannot parse, ignore
            continue

        t_sync = float(t_sync)

        for entry in prof:
            entry['time'] += t_sync


    # we now only have absolute profiles.  Next, we determine the clock
    # skew per host and correct times by that.  We use NTP timestamps where
    # available
    for pname, prof in profs.iteritems():

        if not len(prof):
            # print 'empty profile %s' % pname
            continue

        if not prof[0]['msg']:
            # print 'unsynced profile %s' % pname
            continue

        t_prof = prof[0]['time']

        elems = prof[0]['msg'].split(':')
        if len(elems) == 5:
            host, ip, t_sys, t_ntp, t_mode = elems
            host_id = '%s:%s' % (host, ip)
        elif len(elems) == 4:
            t_sys, _, t_ntp, t_mode = elems
            host_id = 'other'
        else:
            raise ValueError('cannot parse sync timstamp for %s' % pname)

        # the session profile is special - it gives us the session hostid
        if os.path.basename(pname) == '%s.prof' % sid:
            if session_host:
                print 'multiple session hosts: %s, %s' % (session_host, host_id)
            else:
                session_host = host_id

        if t_smin:
            t_smin = min(t_smin, t_ntp)
        else:
            t_smin = t_ntp

        if t_min:
            t_min = min(t_min, t_prof)
        else:
            t_min = t_prof

        # determine the correction for the given host
        t_sys = float(t_sys)
        t_ntp = float(t_ntp)
        t_off = t_sys - t_ntp

        if host_id in t_host:

            accuracy = max(accuracy, t_off-t_host[host_id])

            if abs(t_off-t_host[host_id]) > NTP_DIFF_WARN_LIMIT:
                print 'conflict sync   %-35s (%-35s) %6.1f : %6.1f :  %12.5f' \
                        % (os.path.basename(pname), host_id, t_off, t_host[host_id], (t_off-t_host[host_id]))

            continue # we always use the first match

        t_host[host_id] = t_off

    unsynced = set()
    for pname, prof in profs.iteritems():

        if not len(prof):
            continue

        if not prof[0]['msg']:
            continue

        elems = prof[0]['msg'].split(':')
        if len(elems) == 5:
            host, ip = elems[0:2]
            host_id = '%s:%s' % (host, ip)
        elif len(elems) == 4:
            host_id = 'other'

        if host_id in t_host:
            t_off = t_host[host_id]
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

            # keep track of what hosts any given uid touched
            if host_id:
                if row['event'] == 'advance' and row['state'] == 'PMGR_ACTIVE':
                    uid = row['uid']
                    hostmap[uid] = host_id
                    host_id      = None  # only record once

        # add profile to global one
        p_glob += prof


      # # Check for proper closure of profiling files
      # if c_qed == 0:
      #     print 'WARNING: profile "%s" not correctly closed.' % prof
      # if c_qed > 1:
      #     print 'WARNING: profile "%s" closed %d times.' % (prof, c_qed)

    # sort by time and return
    p_glob = sorted(p_glob[:], key=lambda k: k['time'])

 ## if unsynced:
 ##     print 'unsynced hosts: %s' % list(unsynced)

    return [p_glob, t_smin, hostmap]


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

    profs           = read_profiles(profiles)
    prof, t_min, hm = combine_profiles(profs, sid)
    prof            = clean_profile(prof, sid)

    # fix legacy state names
    for p in prof:
        p['state'] = rps._legacy_states.get(p['state'], p['state'])

    return prof, t_min, hm


# ------------------------------------------------------------------------------
#
def get_session_description(sid, src=None, dburl=None, hostmap=None):
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

    # FIXME: those fixes should be phased out before release
    # for backward compatibility, we convert all `_id` entries into `uid`
    def _fix_uid(this):
        if isinstance(this, dict):
            if '_id' in this and 'uid' not in this:
                this['uid'] = this['_id']
            for x in this:
                _fix_uid(this[x])
        elif isinstance(this, list):
            for x in this:
                _fix_uid(x)

    # also make sure we always have a config
    def _fix_cfg(this):
        if isinstance(this, dict):
            if 'uid' in this and 'cfg' not in this:
                this['cfg'] = dict()
            for x in this:
                _fix_cfg(this[x])
        elif isinstance(this, list):
            for x in this:
                _fix_cfg(x)

    # fix some other names...
    def _fix_names(this):
        if isinstance(this, dict):
            if 'pilotmanager' in this and 'pmgr' not in this:
                this['pmgr'] = this['pilotmanager']
            if 'unitmanager' in this and 'umgr' not in this:
                this['umgr'] = this['unitmanager']
            for x in this:
                _fix_names(this[x])
        elif isinstance(this, list):
            for x in this:
                _fix_names(x)


    _fix_uid(json)
    _fix_cfg(json)
    _fix_names(json)

    assert(sid == json['session']['uid'])

    if not hostmap:
        hostmap     = dict()
    ret             = dict()
    ret['entities'] = dict()

    tree      = dict()
    tree[sid] = {'uid'        : sid,
                 'etype'      : 'session',
                 'cfg'        : json['session']['cfg'],
                 'description': dict(),
                 'has'        : ['umgr', 'pmgr'],
                 'children'   : list()
                }

    for pmgr in sorted(json['pmgr'], key=lambda k: k['uid']):
        uid = pmgr['uid']
        tree[sid]['children'].append(uid)
        tree[uid] = {'uid'        : uid,
                     'etype'      : 'pmgr',
                     'cfg'        : pmgr['cfg'],
                     'description': dict(),
                     'has'        : ['pilot'],
                     'children'   : list()
                    }

    for umgr in sorted(json['umgr'], key=lambda k: k['uid']):
        uid = umgr['uid']
        tree[sid]['children'].append(uid)
        tree[uid] = {'uid'        : uid,
                     'etype'      : 'umgr',
                     'cfg'        : umgr['cfg'],
                     'description': dict(),
                     'has'        : ['unit'],
                     'children'   : list()
                    }

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
        if uid not in hostmap:
            hostmap[uid] = 'other'

    for unit in sorted(json['unit'], key=lambda k: k['uid']):
        uid  = unit['uid']
        pid  = unit['pilot']
        umgr = unit['umgr']
        if pid: tree[pid ]['children'].append(uid)
        tree[umgr]['children'].append(uid)
        tree[uid] = {'uid'         : uid,
                     'etype'       : 'unit',
                     'cfg'         : unit['description'],
                     'description' : unit['description'],
                     'has'         : list(),
                     'children'    : list()
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
            'state_model'  : None,  # session has no states, only events
            'state_values' : None,
            'event_model'  : dict(),
            }

    ret['config'] = dict()  # magic to get session config goes here

    return ret, hostmap


# ------------------------------------------------------------------------------

