
import os

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
                frame = get_profile_frame (prof)
                exp_frames[exp].append ([frame, label])
                
    return exp_frames


# ------------------------------------------------------------------------------
#
def get_profile_frame (prof):
    import pandas as pd
    return pd.read_csv(prof)

# ------------------------------------------------------------------------------
#
tmp = None
def add_concurrency (frame, tgt, spec):
    """
    add a column 'tgt' which is a cumulative sum of conditionals of enother row.  
    
    The purpose is the following: if a unit enters a component, the tgt row counter is 
    increased by 1, if the unit leaves the component, the counter is decreases by 1.
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
                         {'state' :'Cancelled'}]
               }
        get_concurrency (df, 'concurrently_running', spec)
    """
    
    import numpy

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
        return  0
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
        else           : x   = numpy.NaN
        return x


    # --------------------------------------------------------------------------
    # sanitize concurrency: negative values indicate incorrect event ordering,
    # so we set the repesctive values to 0
    # --------------------------------------------------------------------------
    def _abs (x):
        if x < 0:
            return numpy.NaN
        return x
    # --------------------------------------------------------------------------
    
    frame[tgt] = frame.apply(lambda row: _conc(row, spec), axis=1).cumsum()
    frame[tgt] = frame.apply(lambda row: _abs (row[tgt]),  axis=1)
    frame[tgt] = frame.apply(lambda row: _time(row[tgt]),  axis=1)
  # print frame[[tgt, 'time']]


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

