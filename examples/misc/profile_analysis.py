#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import pprint
import collections   as coll

import radical.pilot as rp
import radical.utils as ru

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug 
# set if you want to see what happens behind the scences!


RUNTIME  =    20  # how long to run the pilot
CORES    =    64  # how many cores to use for one pilot
UNITS    =   128  # how many units to create
SLEEP    =     0  # how long each unit sleeps
SCHED    = rp.SCHED_DIRECT_SUBMISSION

#------------------------------------------------------------------------------
#
def create_event_filter():

    # create a dict which orders states
    unit_state_value = coll.OrderedDict()
    # unit_state_value[rp.SCHEDULING                   ] =   5
    # unit_state_value[rp.STAGING_INPUT                ] =   6
    unit_state_value[rp.AGENT_STAGING_INPUT_PENDING  ] =   7
    unit_state_value[rp.AGENT_STAGING_INPUT          ] =   8
    unit_state_value[rp.ALLOCATING_PENDING           ] =   9
    unit_state_value[rp.ALLOCATING                   ] =  10
    unit_state_value[rp.EXECUTING_PENDING            ] =  11
    unit_state_value[rp.EXECUTING                    ] =  12
    unit_state_value[rp.AGENT_STAGING_OUTPUT_PENDING ] =  13
    unit_state_value[rp.AGENT_STAGING_OUTPUT         ] =  14
    unit_state_value[rp.PENDING_OUTPUT_STAGING       ] =  15
    # unit_state_value[rp.STAGING_OUTPUT               ] =  16
    unit_state_value[rp.DONE                         ] =  17
    
    # also create inverse dict
    inv_unit_state_value = coll.OrderedDict()
    for k, v in unit_state_value.items():
        inv_unit_state_value[v] = k
    
    # create a filter which catches transitions between states
    event_filter = coll.OrderedDict()
    for val in inv_unit_state_value:
        s_in  = inv_unit_state_value[val]
        s_out = inv_unit_state_value.get(val+1)
    
        # nothing transitions out of last state
        if not s_out:
            continue
    
      # not interested in pending states...
      # if 'pending' in s_in.lower():
      #     continue
    
        event_filter[s_in] = {'in' : [{'state' : s_in, 
                                       'event' : 'advance',
                                       'msg'   : ''}],
                              'out': [{'state' : s_out, 
                                       'event' : 'advance'}]}

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # we use a reporter class for nicer output
    report = ru.Reporter("Getting Started")

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:
        report.info('read configs')
        resources = ru.read_json('%s/config.json', os.path.dirname(__file__))
        report.ok('\\ok\n')

        report.header('submit pilots')

        # prepare some input files for the compute units
        os.system ('hostname > file1.dat')
        os.system ('date     > file2.dat')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        pdescs = list()
        for resource in sys.argv[1:]:
            pdesc = rp.ComputePilotDescription()
            pdesc.resource      = resource
            pdesc.cores         = CORES
            pdesc.project       = resources[resource]['project']
            pdesc.queue         = resources[resource]['queue']
            pdesc.runtime       = RUNTIME
            pdesc.cleanup       = False
            pdesc.access_schema = resources[resource]['schema']
            pdescs.append(pdesc)

        # Launch the pilot.
        pilots = pmgr.submit_pilots(pdescs)
    
        report.header('submit units')

        session.close (cleanup=False)

    # fetch profiles and convert into inspectable data frames
    if session:
        session_id = session.uid


    event_filter = create_event_filter()

    # we have a session
    profiles   = rp.utils.fetch_profiles(sid=session_id, tgt='/tmp/')
    
    # read all profiles, recombine, and create dataframe
    profs = sys.argv[1:]
    prof  = rp.utils.combine_profiles(profs)
    frame = rp.utils.prof2frame(prof)
    
    # for all filters defined, create the respective rate, event and conc columns,
    # and store them in a separate frame for plotting
    plot_frames = list()
    for state in event_filter.keys():
        rp.utils.add_frequency   (frame, tgt='rate'  , spec=event_filter[state]['in'])
        rp.utils.add_event_count (frame, tgt='events', spec=event_filter[state]['in'])
        rp.utils.add_concurrency (frame, tgt='conc'  , spec=event_filter[state])
      # rp.utils.calibrate_frame (frame,               spec=event_filter[state]['in'])
    
        tmp_df = frame[['time', 'rate', 'events', 'conc']]
        plot_frames.append([tmp_df, state])
        
    # create plots for each type (rate, events, concurrency)
    rp.utils.frame_plot(plot_frames, logx=False, logy=True,
                   title="Throughput", legend=True,
                   axis=[['time', 'time (s)'],
                         ['rate', "rate units/s"]])
    
    rp.utils.frame_plot(plot_frames, logx=False, logy=False,
                   title="State Transitions", legend=True,
                   axis=[['time',  'time (s)'],
                         ['events', "#events"]])
    
    rp.utils.frame_plot(plot_frames, logx=False, logy=False,
                   title="Unit Concurrency", legend=True,
                   axis=[['time', 'time (s)'],
                         ['conc', '#concurrent units']])
    

#-------------------------------------------------------------------------------

