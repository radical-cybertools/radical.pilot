#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys
import pprint
import collections   as coll

os.environ['RADICAL_PILOT_VERBOSE'] = 'REPORT'
os.environ['RADICAL_PILOT_PROFILE'] = 'TRUE'

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#
def create_event_filter():

    # create a dict which orders states
    unit_state_value = coll.OrderedDict()
  # unit_state_value[rp.SCHEDULING                   ] =   5
  # unit_state_value[rp.STAGING_INPUT                ] =   6
  # unit_state_value[rp.AGENT_STAGING_INPUT_PENDING  ] =   7
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
    return event_filter


#------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.LogReporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: resource = sys.argv[1]
    else                   : resource = 'local.localhost'

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # read the config used for resource details
        report.info('read config')
        config = ru.read_json('%s/../config.json' % os.path.dirname(os.path.abspath(__file__)))
        report.ok('>>ok\n')

        report.header('submit pilots')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        report.info('create pilot description')
        pd_init = {
                'resource'      : resource,
                'cores'         : 64,  # pilot size
                'runtime'       : 15,  # pilot runtime (min)
                'exit_on_error' : True,
                'project'       : config[resource]['project'],
                'queue'         : config[resource]['queue'],
                'access_schema' : config[resource]['schema']
                }
        pdesc = rp.ComputePilotDescription(pd_init)
        report.ok('>>ok\n')

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)


        report.header('submit units')

        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        # Create a workload of ComputeUnits.
        # Each compute unit runs '/bin/date'.

        n = 128   # number of units to run
        report.info('create %d unit description(s)\n\t' % n)

        cuds = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            # trigger an error now and then
            if not i % 10: cud.executable = '/bin/data' # does not exist
            else         : cud.executable = '/bin/date'

            cuds.append(cud)
            report.progress()
        report.ok('>>ok\n')

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')
        umgr.wait_units()
    
        report.info('\n')
        for unit in units:
            if unit.state == rp.FAILED:
                report.plain('  * %s: %s, exit: %3s, err: %s' \
                        % (unit.uid, unit.state[:4], 
                           unit.exit_code, unit.stderr.strip()[-35:]))
                report.error('>>err\n')
            else:
                report.plain('  * %s: %s, exit: %3s, out: %s' \
                        % (unit.uid, unit.state[:4], 
                            unit.exit_code, unit.stdout.strip()[:35]))
                report.ok('>>ok\n')
    

    except Exception as e:
        # Something unexpected happened in the pilot code above
        report.error('caught Exception: %s\n' % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        report.warn('exit requested\n')

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        session.close(cleanup=False)

    report.header()


    # fetch profiles and convert into inspectable data frames
    if not session:
        sys.exit(-1)

    event_filter = create_event_filter()

    # we have a session - fetch profiles
    sid   = session.uid
    profs = rp.utils.fetch_profiles(sid=sid, tgt='/tmp/')
    print "---"
    for p in profs:
        print p
    print "---"
    
    # read all profiles, recombine, and create dataframe
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

