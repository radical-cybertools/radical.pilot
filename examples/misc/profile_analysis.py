#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

os.environ['RADICAL_PILOT_VERBOSE'] = 'REPORT'
os.environ['RADICAL_PILOT_PROFILE'] = 'TRUE'

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------

# we use a reporter class for nicer output
report = ru.Reporter(name='radical.pilot')
report.title('Getting Started (RP version %s)' % rp.version)


# ------------------------------------------------------------------------------
#
def profile_analysis(sid):

    import radical.pilot.utils as rpu

    report.header('profile analysis')

    # fetch profiles for all pilots
    profiles   = rpu.fetch_profiles(sid=sid, tgt='/tmp/')
    print(profiles)

    # combine into a single profile
    profile    = rpu.combine_profiles(profiles)

    # derive a global data frame
    frame      = rpu.prof2frame(profile)

    # split into session / pilot / unit frames
    sf, pf, uf = rpu.split_frame(frame)

    print(len(sf))
    print(len(pf))
    print(len(uf))

    print(sf[0:10])
    print(pf[0:10])
    print(uf[0:10])


    # derive some additional 'info' columns, which contains some commonly used
    # tags
    rpu.add_info(uf)

    for index, row in uf.iterrows():
        if str(row['info']) != 'nan':
            print("%-20s : %-10s : %-25s : %-20s" %
                    (row['time'], row['uid'], row['state'], row['info']))

    # add a 'state_from' columns which signals a state transition
    rpu.add_states(uf)
    adv = uf[uf['event'].isin(['advance'])]
    print('---------------')
    print(len(adv))
    print(uf[uf['uid'] == 'unit.000001'])
    print(list(pf['event']))

    tmp = uf[uf['uid'] == 'unit.000001'].dropna()
    print(tmp[['time', 'uid', 'state', 'state_from']])

    # add a columns 'rate_out' which contains the rate (1/s) of the event
    # 'advance to state STAGING_OUTPUT'
    print('---------------')
    rpu.add_frequency(adv, 'rate_out', 0.5, {'state' : 'StagingOutput',
                                             'event' : 'advance'})
    print(adv[['time', 'rate_out']].dropna(subset=['rate_out']))
    print('---------------')

    fig, plot = rpu.create_plot()
    plot.set_title('rate of ouput staging transitions', y=1.05, fontsize=18)

    plot.set_xlabel('time (s)', fontsize=14)
    plot.set_ylabel('rate (1/s)', fontsize=14)
    plot.set_frame_on(True)

    adv[['time', 'rate_out']].dropna().plot(ax=plot, logx=False, logy=False,
            x='time', y='rate_out',
            drawstyle='steps',
            label='output rate', legend=False)

    plot.legend(labels=['output rate'], loc='best', fontsize=14, frameon=True)

    fig.savefig('profile.png', bbox_inches='tight')


# -------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    resource = 'local.localhost'

    # use the sid specified as argument, otherwise run a test
    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [sid]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: sid = sys.argv[1]
    else                   : sid = None

    if sid:
        profile_analysis(sid)
        sys.exit()


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

    profile_analysis(session.uid)
    sys.exit()


# ------------------------------------------------------------------------------

