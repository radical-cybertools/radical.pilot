import os
import csv
import copy
import time
import threading


# ------------------------------------------------------------------------------
#
# "label", "component", "event", "message"
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
    def __init__ (self, name):

        # this init is only called once (globally).  We synchronize clocks and
        # set timestamp_zero

        self._handles = dict()

        # we only profile if so instructed
        if 'RADICAL_PILOT_PROFILE' in os.environ:
            self._enabled = True
        else:
            self._enabled = False
            return

        self._ts_zero, self._ts_abs = self._timestamp_init()

        self._name  = name
        self._handle = open("%s.prof"  % self._name, 'a')

        # write header and time normalization info
        # NOTE: Don't forget to sync any format changes in the bootstrapper
        #       and downstream analysis tools too!
        self._handle.write("#time,name,uid,state,event,msg\n")
        self._handle.write("%.4f,%s:%s,%s,%s,%s,%s\n" % \
                (0.0, self._name, "", "", "", 'sync abs',
                "%s:%s:%s" % (time.time(), self._ts_zero, self._ts_abs)))


    # ------------------------------------------------------------------------------
    #
    @property
    def enabled(self):

        return self._enabled


    # ------------------------------------------------------------------------------
    #
    def flush(self):

        if self._enabled:
            self._handle.flush()


    # ------------------------------------------------------------------------------
    #
    def prof(self, event, uid=None, state=None, msg=None, timestamp=None, logger=None):

        if not self._enabled:
            return

        if logger:
            logger("%s (%10s%s) : %s", event, uid, state, msg)

        if timestamp != None:
            if timestamp > (100 * 1000 * 1000):
                # older than 3 years (time after 1973) 
                # --> this is an absolute timestamp
                timestamp = timestamp - self._ts_zero
            else:
                # this is a relative timestamp -- leave as is
                pass
        else:
            # no timestamp provided -- use 'now'
            timestamp = self._timestamp_now()

        tid = threading.current_thread().name

        if not uid  : uid   = ''
        if not msg  : msg   = ''
        if not state: state = ''


        # NOTE: Don't forget to sync any format changes in the bootstrapper
        #       and downstream analysis tools too!
        self._handle.write("%.4f,%s:%s,%s,%s,%s,%s\n" \
                % (timestamp, self._name, tid, uid, state, event, msg))
        self.flush()


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
            response = ntplib.NTPClient().request('0.pool.ntp.org')
            timestamp_sys  = response.orig_time
            timestamp_abs  = response.tx_time
            return [timestamp_sys, timestamp_abs]
        except:
            t = time.time()
            return [t,t]


    # --------------------------------------------------------------------------
    #
    def _timestamp_now(self):

        # relative timestamp seconds since TIME_ZERO (start)
        return float(time.time()) - self._ts_zero


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
            
            import glob
            for prof in glob.glob ("%s/%s-pilot.*.prof" % (datadir, sid)):
                print "     - %s" % prof
                frame = pd.read_csv(prof)
                exp_frames[exp].append ([frame, label])
                
    return exp_frames


# ------------------------------------------------------------------------------
#
def combine_profiles(profiles):
    """
    We first read all profiles as CSV files and parse them.  For each profile,
    we back-calculate global time (epoch) from the synch timestamps.  Then all
    profiles are merged (time sorted).

    This routine expectes all profiles to have a synchronization time stamp.
    Two kinds of sync timestamps are supported: absolute and relative.  'sync
    abs' events have a message which contains system time and ntp time, and thus
    allow to adjust the whole timeframe toward globally synched 'seconds since 
    epoch' units.  'sync rel' events have messages which have a corresponding
    'sync ref' event in another profile.  When that second profile is 'sync
    abs'ed, then the first profile will be normalized based on the synchronizity
    of the 'sync rel' and 'sync ref' events.

    This method is somewhat convoluted -- I would not be surprised if it can be
    written much shorter and clearer with some python or pandas magic...
    """
    rd_abs = dict() # dict of absolute time refs
    rd_rel = dict() # dict of relative time refs
    pd_abs = dict() # profiles which have absolute time refs
    pd_rel = dict() # profiles which have relative time refs

    for prof in profiles:
        p     = list()
        tref  = None
        with open(prof, 'r') as csvfile:
            reader = csv.DictReader(csvfile, fieldnames=_prof_fields)
            empty  = True
            for row in reader:

                # skip header
                if row['time'].startswith('#'):
                    continue
    
                empty = False
                row['time'] = float(row['time'])
    
                # find first tref
                if not tref:
                    if row['event'] == 'sync rel' : 
                        tref = 'rel'
                        rd_rel[prof] = [row['time'], row['msg']]
                    if row['event'] == 'sync abs' : 
                        tref = 'abs'
                        rd_abs[prof] = [row['time']] + row['msg'].split(':')

                # store row in profile
                p.append(row)
    
        if   tref == 'abs': pd_abs[prof] = p
        elif tref == 'rel': pd_rel[prof] = p
        elif not empty    : print 'WARNING: skipping profile %s (no sync)' % prof
    
    # make all timestamps absolute for pd_abs profiles
    for prof, p in pd_abs.iteritems():
    
        # the profile created an entry t_rel at t_abs.  
        # The offset is thus t_abs - t_rel, and all timestamps 
        # in the profile need to be corrected by that to get absolute time
        t_rel   = float(rd_abs[prof][0])
        t_stamp = float(rd_abs[prof][1])
        t_zero  = float(rd_abs[prof][2])
        t_abs   = float(rd_abs[prof][3])
        t_off   = t_abs - t_rel
    
        for row in p:
            row['time'] = row['time'] + t_off
    
    # combine the abs profiles into a global one.  We will add rel rpfiles as
    # they are corrected.
    p_glob = list()
    for prof, p in pd_abs.iteritems():
        p_glob += p

    
    # reference relative profiles
    for prof, p in pd_rel.iteritems():
    
        # a sync message was created at time t_rel
        t_rel = rd_rel[prof][0]
        t_msg = rd_rel[prof][1]
    
        # now find the referenced sync point in other, absolute profiles
        t_ref = None
        for _prof, _p in pd_abs.iteritems():
            if not t_ref:
                for _row in _p:
                    if  _row['event'] == 'sync ref' and \
                        _row['msg']   == t_msg:
                        t_ref = _row['time'] # referenced timestamp
                        break
    
        if t_ref == None:
            print "WARNING: 'sync rel' reference not found %s" % prof
            continue
    
        # the profile's sync reference t_rel was created at the t_abs of the
        # referenced point (t_ref), so all timestamps in the profile need to be
        # corrected by (t_ref - t_rel)
        t_off = t_ref - t_rel
    
        for row in p:
            row['time'] = row['time'] + t_off
            p_glob.append(row)

    # we now have all profiles combined into one large profile, and can make
    # timestamps relative to its smallest timestamp again
    
    # find the smallest time over all profiles
    t_min = 9999999999.9 # future...
    for row in p_glob:
        t_min = min(t_min, row['time'])
    
    # make times relative to t_min again
    for row in p_glob:
        row['time'] -= t_min
    
    # sort by time and return
    p_glob = sorted(p_glob[:], key=lambda k: k['time']) 

    return p_glob


# ------------------------------------------------------------------------------
#
def blowup(config, cus, component, logger=None):
    # for each cu in cu_list, add 'factor' clones just like it, just with
    # a different ID (<id>.clone_001)
    #
    # This method also *drops* clones as needed!
    #
    # return value: [list of original and expanded CUs, list of dropped CUs]

    # TODO: I dont like it that there is non blow-up semantics in the blow-up function.
    # Probably want to put the conditional somewhere else.
    if not isinstance (cus, list) :
        cus = [cus]

    # blowup is only enabled on profiling
    if 'RADICAL_PILOT_PROFILE' not in os.environ:
        return

    factor = config['blowup_factor'].get (component, 1)
    drop   = config['drop_clones']  .get (component, 1)

    # FIXME
  # prof ("debug", msg="%s drops with %s" % (component, drop))

    cloned  = list()
    dropped = list()

    for cu in cus :

        uid = cu['_id']

        if drop >= 1:
            # drop clones --> drop matching uid's
            if '.clone_' in uid :
                # FIXME
              # prof ('drop clone', msg=component, uid=uid)
                dropped.append(cu)
                continue

        if drop >= 2:
            # drop everything, even original units
            # FIXME
          # prof ('drop', msg=component, uid=uid)
            dropped.append(cu)
            continue

        if factor < 0:
            # FIXME: we should print a warning or something?
            # Anyway, we assume the default here, ie. no blowup, no drop.
            factor = 1

        for idx in range(factor-1) :

            cu_clone = copy.deepcopy (dict(cu))
            clone_id = '%s.clone_%05d' % (str(cu['_id']), idx+1)

            for key in cu_clone :
                if isinstance (cu_clone[key], basestring) :
                    cu_clone[key] = cu_clone[key].replace (uid, clone_id)

            idx += 1
            cloned.append(cu_clone)
            # FIXME
          # prof('add clone', msg=component, uid=clone_id)

        # For any non-zero factor, append the original unit -- factor==0 lets us
        # drop the cu.
        #
        # Append the original cu last, to increase the likelyhood that
        # application state only advances once all clone states have also
        # advanced (they'll get pushed onto queues earlier).  This cannot be
        # relied upon, obviously.
        if factor > 0: cloned.append(cu)

    return cloned, dropped


# ------------------------------------------------------------------------------

