#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

verbose  = os.environ.get('RADICAL_PILOT_VERBOSE', 'REPORT')
os.environ['RADICAL_PILOT_VERBOSE'] = verbose

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------


#------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if len(sys.argv) >= 2  : resources = sys.argv[1:]
    else                   : resources = ['local.localhost']

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
        config = ru.read_json('%s/config.json' % os.path.dirname(os.path.abspath(__file__)))
        report.ok('>>ok\n')

        report.header('submit pilots')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pdescs = list()
        for resource in resources:
            pd_init = {
                    'resource'      : resource,
                    'runtime'       : 15,  # pilot runtime (min)
                    'exit_on_error' : True,
                    'project'       : config[resource]['project'],
                    'queue'         : config[resource]['queue'],
                    'access_schema' : config[resource]['schema'],
                    'cores'         : config[resource]['cores'],
                    }
            pdescs.append(rp.ComputePilotDescription(pd_init))

        # Launch the pilots.
        pilots = pmgr.submit_pilots(pdescs)


        report.header('submit units')

        # use different schedulers, depending on number of pilots
        report.info('select scheduler')
        if len(pilots) in [1, 2]:
            SCHED = rp.SCHEDULER_ROUND_ROBIN
        else:
            SCHED = rp.SCHEDULER_BACKFILLING
        report.ok('>>%s\n' % SCHED)
    
        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager(session=session, scheduler=SCHED)
        umgr.add_pilots(pilots)

        # Create a workload of ComputeUnits.
        # Each compute unit reports the id of the pilot it runs on.

        n = 256 # number of units to run
        report.info('create %d unit description(s)\n\t' % n)

        cuds = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            cud.executable = '/bin/echo'
            cud.arguments  = ['$RP_PILOT_ID']

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
        counts = dict()
        for unit in units:
            out_str = unit.stdout.strip()[:35]
            report.plain('  * %s: %s, exit: %3s, out: %s\n' \
                    % (unit.uid, unit.state[:4], 
                        unit.exit_code, out_str))
            if out_str not in counts:
                counts[out_str] = 0
            counts[out_str] += 1

        report.info("\n")
        for out_str in counts:
            report.info("  * %-20s: %3d\n" % (out_str, counts[out_str]))
        report.info("  * %-20s: %3d\n" % ('total', sum(counts.values())))
    

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
        session.close()

    report.header()


#-------------------------------------------------------------------------------

