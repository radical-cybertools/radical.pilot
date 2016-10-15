import os
import csv
import copy
import time
import threading


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

