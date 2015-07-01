
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
                frame = pd.read_csv(prof)
                exp_frames[exp].append ([frame, label])
                
    return exp_frames


# ------------------------------------------------------------------------------
#
tmp = None
def add_concurrency (frame, tgt, src, test_in, test_out):
    # add a column 'tgt' which is a cumulative sum of conditionals of enother row.  
    # 
    # The purpose is the following: if a unit enters a component, the tgt row counter is 
    # increased by 1, if the unit leaves the component, the counter is decreases by 1.
    # For any time, the resulting row contains the number of units which is in the 
    # component.  Or state.  Or whatever.
    #
    # The arguments are:
    #   tgt: name of the new column
    #   src: name of the column to check for condition
    #   test_in:  list of conditions to increase the cumsum (if src.value in test_in )
    #   test_out: list of conditions to decrease the cumsum (if src.value in test_out)
    #
    # Example:
    # get_concurrency ('concurrently_running', 'state', ['Executing'], ['Done', 'Failed', 'Canceled']
    # 
    # create a temporary row over which we can do the commulative sum
    # --------------------------------------------------------------------------
    def _conc (x, xin, xout):
        if   x in xin : return  1
        elif x in xout: return -1
        else          : return  0
    # --------------------------------------------------------------------------

    # we only want to later look at changes of the concurrency -- leading or trailing 
    # idle times are to be ignored.  We thus set repeating values of the cumsum to NaN, 
    # so that they can be filtered out when ploting: df.dropna().plot(...).  
    # That specifically will limit the plotted time range to the area of activity. 
    # The full time range can still be plotted when ommitting the dropna() call.
    # --------------------------------------------------------------------------
    def _time (x):
        global tmp
        import numpy
        if     x != tmp: tmp = x
        else           : x   = numpy.NaN
        return x
    # --------------------------------------------------------------------------
   
    frame[tgt] = frame.apply(lambda row: _conc(row[src], test_in, test_out), axis=1).cumsum()
    frame[tgt] = frame.apply(lambda row: _time(row[tgt]), axis=1)


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
def frame_plot (frames, axis, title=None, logx=False, logy=False, figdir=None):
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
        frame.dropna().plot(ax=plot, logx=logx, logy=logy, x=axis[0][0], y=axis[1][0], label=label)
        labels.append(label)
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

