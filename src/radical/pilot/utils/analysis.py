
import os

from ..states import *

info_names = {
        'AgentWorker'                 : 'awo',
        'AgentStagingInputComponent'  : 'asic',
        'SchedulerContinuous'         : 'asc',  # agent scheduler component
        'AgentExecutingComponent'     : 'aec',
        'AgentStagingOutputComponent' : 'asoc',
        'session'                     : 'mod'
        }

_info_events = {
        'get'        : '_get_u',     # get a unit from a queue
        'work start' : '_work_u',    # unit is handed over to component
        'work done'  : '_worked_u',  # component finished to operate on unit
        'put'        : '_put_u',     # unit is put onto the next queue
        'publish'    : '_pub_u',     # unit state is published via some pubsub
        'advance'    : '_adv_u',     # the unit state is advanced
        'update'     : '_upd_u'      # a unit state update is pushed to the DB
        }

_info_pending = {
        'Pending'    : '_pend'
        }

_info_premature_final = {
        'Failed'   : '_fail',
        'Canceled'   : '_canc'
}

_info_states = [
        ACTIVE,
        AGENT_STAGING_INPUT,
        AGENT_STAGING_INPUT_PENDING,
        AGENT_STAGING_OUTPUT,
        AGENT_STAGING_OUTPUT_PENDING,
        ALLOCATING,
        ALLOCATING_PENDING,
        CANCELED,
        DONE,
        EXECUTING,
        EXECUTING_PENDING,
        FAILED,
        LAUNCHING,
        NEW,
        PENDING,
        PENDING_ACTIVE,
        PENDING_EXECUTION,
        PENDING_INPUT_STAGING,
        PENDING_LAUNCH,
        PENDING_OUTPUT_STAGING,
        SCHEDULING,
        STAGING_INPUT,
        STAGING_OUTPUT,
        UNSCHEDULED
        ]

_info_entries = [
    ('umgr_get_u',      'MainThread',             'advance',   'New'),
    ('umgr_adv_u_pend', 'MainThread',             'advance',   'PendingInputStaging'),
    ('usic_get_u',      'InputFileTransfer',      'advance',   'StagingInput'),
    ('usic_adv_u_pend', 'InputFileTransfer',      'advance',   'AgentStagingInputPending'),

    ('usoc_get_u',      'OutputFileTransfer',     'advance',   'StagingOutput'),
    ('usoc_adv_u',      'OutputFileTransfer',     'advance',   'Done'),

    # FIXME: the names below will break for other schedulers
    ('asc_allocated',   'SchedulerContinuous',    'schedule',  'allocated'),
    ('asc_alloc_nok',   'SchedulerContinuous',    'schedule',  'allocation failed'),
    ('asc_alloc_ok',    'SchedulerContinuous',    'schedule',  'allocation succeeded'),
    ('asc_unqueue',     'SchedulerContinuous',    'unqueue',   're-allocation done'),
    ('asc_released',    'SchedulerContinuous',    'unschedule','released'),

    ('aec_launch',      'AgentExecuting',         'exec',      'unit launch'),
    ('aec_spawn',       'AgentExecuting',         'spawn',     'unit spawn'),
    ('aec_script',      'AgentExecuting',         'command',   'launch script constructed'),
    ('ace_outerr',      'AgentExecuting',         'command',   'stdout and stderr files created'),
    ('aec_handover',    'AgentExecuting',         'spawn',     'spawning passed to pty'),
    ('aec_handover',    'AgentExecuting',         'spawn',     'spawning passed to popen'),
    ('aec_end',         'AgentExecuting',         'final',     ''),

    ('aec_pickup',      'AgentExecuting',         'passed',    'ExecWatcher picked up unit'),
    ('aec_start_script','AgentStagingOutputComponent','script','start_script'),
    ('aec_after_cd',    'AgentStagingOutputComponent','script','after_cd'),
    ('aec_after_exec',  'AgentStagingOutputComponent','script','after_exec'),
    ('aec_complete',    'AgentExecuting',         'exec',      'execution complete'),
]

# ------------------------------------------------------------------------------
#
tmp = None
def add_concurrency (frame, tgt, spec):
    """
    add a column 'tgt' which is a cumulative sum of conditionals of another row.
    
    The purpose is the following: if a unit enters a component, the tgt row counter is 
    increased by 1, if the unit leaves the component, the counter is decreased by 1.
    For any time, the resulting row contains the number of units which is in the 
    component.  Or state.  Or whatever.
    
    The arguments are:
        'tgt'  : name of the new column
        'spec' : a set of filters to determine if a unit enters or leaves
    
    'spec' is expected to be a dict of the following format:
    
        spec = { 'in'  : [{'col1' : 'pat1', 
                           'col2' : 'pat2'},
                          ...],
                 'out' : [{'col3' : 'pat3', 
                           'col4' : 'pat4'},
                          ...]
               }
    
    where:
        'in'    : filter set to determine the unit entering
        'out'   : filter set to determine the unit leaving
        'col'   : name of column for which filter is defined
        'event' : event which correlates to entering/leaving
        'msg'   : qualifier on the event, if event is not unique
    
    Example:
        spec = {'in'  : [{'state' :'Executing'}],
                'out' : [{'state' :'Done'},
                         {'state' :'Failed'},
                         {'state' :'Canceled'}]
               }
        add_concurrency (df, 'concurrently_running', spec)
    """
    
    import numpy as np

    # create a temporary row over which we can do the commulative sum
    # --------------------------------------------------------------------------
    def _conc (row, spec):

        # row must match any filter dict in 'spec[in/out]' 
        # for any filter dict it must match all col/pat pairs

        # for each in filter
        for f in spec['in']:
            match = 1 
            # for each col/val in that filter
            for col, pat in f.iteritems():
                if row[col] != pat:
                    match = 0
                    break
            if match:
                # one filter matched!
              # print " + : %-20s : %.2f : %-20s : %s " % (row['uid'], row['time'], row['event'], row['message'])
                return 1

        # for each out filter
        for f in spec['out']:
            match = 1 
            # for each col/val in that filter
            for col, pat in f.iteritems():
                if row[col] != pat:
                    match = 0
                    break
            if match:
                # one filter matched!
              # print " - : %-20s : %.2f : %-20s : %s " % (row['uid'], row['time'], row['event'], row['message'])
                return -1

        # no filter matched
      # print "   : %-20s : %.2f : %-20s : %s " % (row['uid'], row['time'], row['event'], row['message'])
        return  np.NaN
    # --------------------------------------------------------------------------

    # we only want to later look at changes of the concurrency -- leading or trailing 
    # idle times are to be ignored.  We thus set repeating values of the cumsum to NaN, 
    # so that they can be filtered out when ploting: df.dropna().plot(...).  
    # That specifically will limit the plotted time range to the area of activity. 
    # The full time range can still be plotted when ommitting the dropna() call.
    # --------------------------------------------------------------------------
    def _time (x):
        global tmp
        if     x != tmp: tmp = x
        else           : x   = np.NaN
        return x


    # --------------------------------------------------------------------------
    # sanitize concurrency: negative values indicate incorrect event ordering,
    # so we set the repesctive values to 0
    # --------------------------------------------------------------------------
    def _abs (x):
        if x < 0:
            return np.NaN
        return x
    # --------------------------------------------------------------------------
    
    frame[tgt] = frame.apply(lambda row: _conc(row, spec), axis=1).cumsum()
    frame[tgt] = frame.apply(lambda row: _abs (row[tgt]),  axis=1)
    frame[tgt] = frame.apply(lambda row: _time(row[tgt]),  axis=1)
  # print frame[[tgt, 'time']]

    return frame



# ------------------------------------------------------------------------------
#
def add_frequency(frame, tgt, window, spec):
    """
    This method will add a row 'tgt' to the given data frame, which will contain
    a contain the frequency (1/s) of the events specified in 'spec'.

    We first will filter the given frame by spec, and then apply a rolling
    window over the time column, counting the rows which fall into the window.
    The result is *not* divided by window size, so normalization is up to the
    caller.
    
    The method looks backwards, so the resulting frequency column contains the
    frequency which applied *up to* that point in time.
    """
    
    # --------------------------------------------------------------------------
    def _freq(t, _tmp, _window):
        # get sequence of frame which falls within the time window, and return
        # length of that sequence
        return len(_tmp.uid[(_tmp.time > t-_window) & (_tmp.time <= t)])
    # --------------------------------------------------------------------------
    
    # filter the frame by the given spec
    tmp = frame
    for key,val in spec.iteritems():
        tmp = tmp[tmp[key].isin([val])]
    frame[tgt] = tmp.time.apply(_freq, args=[tmp, window])

    return frame


# ------------------------------------------------------------------------------
#
def add_event_count(frame, tgt, spec):
    """
    This method will add a row 'tgt' to the given data frame, which will contain
    a counter of the events specified in 'spec'.

    This works similar to add_frequency: we first filter, and then add the
    cumsum. 
    """

    raise NotImplementedError('not yet implemented')

    # --------------------------------------------------------------------------
    def _ecnt(t, _tmp):
        # get sequence of frame which falls within the time window, and return
        # length of that sequence
        return len(_tmp.uid[(_tmp.time <= t)])
    # --------------------------------------------------------------------------

    # filter the frame by the given spec
    tmp = frame
    for key,val in spec.iteritems():
        tmp = tmp[tmp[key].isin([val])]
    frame[tgt] = tmp.time.apply(_ecnt, args=[tmp])

    return frame


# ------------------------------------------------------------------------------
#
t0 = None
def calibrate_frame(frame, spec):
    """
    move the time axis of a profiling frame so that t_0 is at the first event
    matching the given 'spec'.  'spec' has the same format as described in
    'add_concurrency' (list of dicts with col:pat filters)
    """

    # --------------------------------------------------------------------------
    def _find_t0 (row, spec):

        # row must match any filter dict in 'spec[in/out]' 
        # for any filter dict it must match all col/pat pairs
        global t0
        if t0 is not None:
            # already found t0
            return

        # for each col/val in that filter
        for f in spec:
            match = 1 
            for col, pat in f.iteritems():
                if row[col] != pat:
                    match = 0
                    break
            if match:
                # one filter matched!
                t0 = row['time']
                return
    # --------------------------------------------------------------------------

    # --------------------------------------------------------------------------
    def _calibrate (row, t0):

        if t0 is None:
            # no t0...
            return

        return row['time'] - t0
    # --------------------------------------------------------------------------

    # we need to iterate twice over the frame: first to find t0, then to
    # calibrate the time axis
    global t0
    t0 = None # no t0
    frame.apply(lambda row: _find_t0  (row, spec), axis=1)

    if t0 == None:
        print "Can't recalibrate, no matching timestamp found"
        return
    frame['time'] = frame.apply(lambda row: _calibrate(row, t0  ), axis=1)

    return frame


# ------------------------------------------------------------------------------
#
def create_plot():
    """
    create a plot object and tune its layout to our liking.
    """
    
    import matplotlib.pyplot as plt

    fig, plot = plt.subplots(figsize=(12,6))
    
    plot.xaxis.set_tick_params(width=1, length=7)
    plot.yaxis.set_tick_params(width=1, length=7)

    plot.spines['right' ].set_position(('outward', 10))
    plot.spines['top'   ].set_position(('outward', 10))
    plot.spines['bottom'].set_position(('outward', 10))
    plot.spines['left'  ].set_position(('outward', 10))

    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)
    
    fig.tight_layout()

    return fig, plot


# ------------------------------------------------------------------------------
#
def frame_plot (frames, axis, title=None, logx=False, logy=False, 
                legend=True, figdir=None):
    """
    plot the given axis from the give data frame.  We create a plot, and plot
    all frames given in the list.  The list is expected to contain [frame,label]
    pairs
    
    frames: list of tuples of dataframes and labels
    frames  = [[stampede_df_1, 'stampede - popen'], 
               [stampede_df_2, 'stampede - shell'],
               [stampede_df_3, 'stampede - ORTE' ]]
     
    axis:   tuple of data frame column index and axis label
    axis    = ['time', 'time (s)']
    """
    
    # create figure and layout
    fig, plot = create_plot()

    # set plot title
    if title:
        plot.set_title(title, y=1.05, fontsize=18)

    # plot the data frames
    # NOTE: we need to set labels separately, because of
    #       https://github.com/pydata/pandas/issues/9542
    labels = list()
    for frame, label in frames:
        try:
            frame.dropna().plot(ax=plot, logx=logx, logy=logy,
                    x=axis[0][0], y=axis[1][0],
                    drawstyle='steps',
                    label=label, legend=False)
        except Exception as e:
            print "skipping frame '%s': '%s'" % (label, e)

    if legend:
        plot.legend(labels=labels, loc='upper right', fontsize=14, frameon=True)

    # set axis labels
    plot.set_xlabel(axis[0][1], fontsize=14)
    plot.set_ylabel(axis[1][1], fontsize=14)
    plot.set_frame_on(True)
   
    # save as png and pdf.  Use the title as base for names
    if title: base = title
    else    : base = "%s_%s" % (axis[0][1], axis[1][1])
        
    # clean up base name -- only keep alphanum and such
    import re
    base = re.sub('[^a-zA-Z0-9\.\-]', '_', base)
    base = re.sub('_+',               '_', base)
    
    if not figdir:
        figdir = os.getcwd()

    print 'saving %s/%s.png' % (figdir, base)
    fig.savefig('%s/%s.png' % (figdir, base), bbox_inches='tight')

    print 'saving %s/%s.pdf' % (figdir, base)
    fig.savefig('%s/%s.pdf' % (figdir, base), bbox_inches='tight')

    return fig, plot


# ------------------------------------------------------------------------------
#
def create_analytical_frame (idx, kind, args, limits, step):
    """
    create an artificial data frame, ie. a data frame which does not contain
    data gathered from an experiment, but data representing an analytical
    construct of some 'kind'.

    idx:    data frame column index to fill (a time column is always created)
    kind:   construct to use (only 'rate' is supporte right now)
    args:   construct specific parameters
    limits: time range for which data are to be created
    step:   time steps for which data are to be created
    """

    import pandas as pd

    # --------------------------------------------------------------------------
    def _frange(start, stop, step):
        while start <= stop:
            yield start
            start += step
    # --------------------------------------------------------------------------
            
    if kind == 'rate' :
        t_0  = args.get ('t_0',  0.0)
        rate = args.get ('rate', 1.0)
        data = list()
        for t in _frange(limits[0], limits[1], step):
            data.append ({'time': t+t_0, idx: t*rate})
        return pd.DataFrame (data)
        
    else:
        raise ValueError ("No such frame kind '%s'" % kind)
        

# ------------------------------------------------------------------------------
#
def add_derived(df):
    """
    Add additional (derived) colums to dataframes
    create columns based on two other columns using an operator
    """
    
    import operator

    # TODO: The fields these are derived from are outdated by now!
    df['executor_queue'] = operator.sub(df['ewo_get'],      df['as_to_ewo'])
    df['raw_runtime']    = operator.sub(df['ewa_complete'], df['ewo_launch'])
    df['full_runtime']   = operator.sub(df['uw_push_done'], df['as_to_ewo'])
    df['watch_delay']    = operator.sub(df['ewa_get'],      df['ewo_to_ewa'])
    df['allocation']     = operator.sub(df['as_allocated'], df['a_to_as'])

    # add a flag to indicate if a unit / pilot / ... is cloned
    # --------------------------------------------------------------------------
    def _cloned (row):
        return 'clone' in row['uid'].lower()
    # --------------------------------------------------------------------------
    df['cloned'] = df.apply(lambda row: _cloned (row), axis=1)

    return df


# ------------------------------------------------------------------------------
#
def add_info(df):
    """
    we also derive some specific info from the event/msg columns, based on
    the mapping defined in _info_entries.  That should make it easier to
    analyse the data.
    """

    import numpy as np

    # --------------------------------------------------------------------------
    def _info (row):
        for info, name, event, msg in _info_entries:
            if  (row['name'] and name  in row['name'] ) and \
                (not event   or  event == row['event']) and \
                (not msg     or  msg   == row['msg']  ):
                return info

        ret = ""
        n   = 0  # 
        for pat, pre in info_names.iteritems():
            if pat in row['name']:
                ret += pre
                n   += 1
                break
        for pat, ev in _info_events.iteritems():
            if ret and pat == row['event']:
                ret += ev
                n   += 1
                break
        for pat, s in _info_pending.iteritems():
            if ret and pat in row['state']:
                ret += s
                break
        # Also create separate info entries for Canceled and Failed units
        for pat, f in _info_premature_final.iteritems():
            if ret and pat in row['state']:
                ret += f
                break

        if ret and n >= 2:
            return ret
        else:
            return np.NaN
    # --------------------------------------------------------------------------
    df['info'] = df.apply(lambda row: _info (row), axis=1)

    return df
    

# ------------------------------------------------------------------------------
#
def get_info_df(df):
    """
    This call will convert a raw profiling frame into a transposed frame where
    possible info tags are columns, and each row contains the respective timings
    for uni unit.  uid is df index.
    """

    import pandas as pd

    uids  = list(df['uid'].unique())
    cols  = set()
    dicts = dict()

    for uid in uids:
        uf_n  = df[df['uid'] == uid]
        tmp1   = uf_n[['time', 'info']].dropna()
        tmp1_d = tmp1.set_index('info').to_dict()['time']

        # add state transitions to dict.  We basically select all rows with
        # a 'state_from', and add a dict entry for the 'state' column.
        # Make sure we ignore transitions where 'state' == 'state_from'.
        tmp2   = uf_n[uf_n['state_from'].notnull()]
        tmp3   = tmp2[tmp2['state'] != tmp2['state_from']]
        tmp4   = tmp3[['time', 'state']].dropna()
        tmp2_d = tmp4.set_index('state').to_dict()['time']
        for k,v in tmp2_d.iteritems():
            tmp1_d[k] = v
        # make sure the frame column headers are complete
        for s in _info_states:
            cols.add(s)

        # make sure we got no info double defined on any unit.  Also derive
        # column names
        l = list(uf_n['info'].dropna())
        for i in set(l):
            cols.add(i)
            if l.count(i)>1:
                raise ValueError('doubled info entry %s (uid:%s)' % (i, uid))

        dicts[uid] = tmp1_d


    new_df = pd.DataFrame(columns=cols, index=uids)
    for uid in dicts:
        new_df.loc[uid] = pd.Series(dicts[uid])

    return new_df


# ------------------------------------------------------------------------------
#
def get_state_df(df):
    """
    This call accepts an 'info' df, and reduces it to only contain state
    transitions.
    """

    return df[_info_states]


# ------------------------------------------------------------------------------
#
def add_states(df):
    """
    Add one additional columns: 'state_from'.  It will have a value for all
    columns where a stateful entity entered a new state, and will have the value
    of the previous state.

    We also fill out the state column, to continue to have the value of any
    previous state setting.
    """
    
    import numpy as np

    # --------------------------------------------------------------------------
    _old_states = dict()
    def _state_from (row):
        old = np.NaN
        if  row['uid']   and \
            row['state'] and \
            row['event'] == 'advance': 
            old = _old_states.get(row['uid'], np.NaN)
            _old_states[row['uid']] = row['state']
        return old
    # --------------------------------------------------------------------------
  # df['state_from'], df['state_to'] = zip(*df.apply(lambda row: _state(row), axis=1))
    df['state_from'] = df.apply(lambda row: _state_from(row), axis=1)

    _old_states = dict()
    # --------------------------------------------------------------------------
    def _state (row):
        if  not row['uid']:
            return np.NaN
        if row['state']:
            _old_states[row['uid']] = row['state']
        return _old_states.get(row['uid'], '')
    # --------------------------------------------------------------------------
    df['state'] = df.apply(lambda row: _state(row), axis=1)

    return df


# ------------------------------------------------------------------------------
#
def get_span(df, spec):
    """
    get the timespan from the first of any of the listed events to the last of
    any of the listed events.  Events are 'info' entries.
    """
    
    import numpy as np

    if not isinstance(spec, list):
        spec = [spec]

    first = float(df.tail(1)['time']) # max
    last  = float(df.head(1)['time']) # min

    ok = False
    for s in spec:
        tmp   = df[df['info'] == s]
        if len(tmp):
            ok = True
            first = min(first, float(tmp.head(1).iloc[0]['time']))
            last  = max(last,  float(tmp.tail(1).iloc[0]['time']))

    if not ok:
        return None

    return last - first


# ------------------------------------------------------------------------------

