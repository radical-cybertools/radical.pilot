#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if len(sys.argv) >= 2: resources = sys.argv[1:]
    else                 : resources = ['local.localhost']

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

        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)

        n = 1
        pdescs = list()
        for resource in resources:

            # Define an [n]-core local pilot that runs for [x] minutes
            # Here we use a dict to initialize the description object
            for i in range(n):
                pd_init = {
                      'resource'      : resource,
                      'runtime'       : 60,   # pilot runtime (min)
                      'exit_on_error' : True,
                      'project'       : config.get(resource,{}).get('project'),
                      'queue'         : config.get(resource,{}).get('queue'),
                      'access_schema' : config.get(resource,{}).get('schema'),
                      'cores'         : config[resource]['cores'],
                }
                pdesc = rp.ComputePilotDescription(pd_init)
                pdescs.append(pdesc)

        # Launch the pilot.
        pilots = pmgr.submit_pilots(pdescs)
        umgr.add_pilots(pilots)

        report.header('submit units')


        # Create a workload of ComputeUnits.
        # Each compute unit runs '/bin/date'.
        n = 128  # number of units to run
        report.info('create %d unit description(s)\n\t' % n)

        cuds = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            if i % 10:
                cud.executable = '/bin/date'
            else:
                # trigger an error now and then
                cud.executable = '/bin/data'  # does not exist
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
            if unit.state in [rp.FAILED, rp.CANCELED]:
                report.plain('  * %s: %s, exit: %5s, err: %35s'
                            % (unit.uid, unit.state[:4],
                               unit.exit_code, unit.stderr))
                report.error('>>err\n')

            else:
                report.plain('  * %s: %s, exit: %5s, out: %35s'
                            % (unit.uid, unit.state[:4],
                               unit.exit_code, unit.stdout))
                report.ok('>>ok\n')


    except Exception as e:
        # Something unexpected happened in the pilot code above
        session._log.exception('oops')
        report.error('caught Exception: %s\n' % e)
        raise

    except (KeyboardInterrupt, SystemExit):
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        report.warn('exit requested\n')

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        if session:
            session.close(cleanup=False)

    report.header()


# ------------------------------------------------------------------------------

