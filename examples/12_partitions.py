#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys
import time

import radical.pilot as rp
import radical.utils as ru

dh = ru.DebugHelper()

# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.LogReporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # This example will start a pilot with two partitions, where each partition
    # occupies 25% and 75% of the total resources, respectively.

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
        config = ru.read_json('%s/config.json' % os.path.dirname(os.path.abspath(__file__)))
        report.ok('>>ok\n')

        report.header('submit pilots')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        total_cores = config[resource]['cores']
        part1_cores = int(total_cores * 0.25)
        part2_cores = total_cores - part1_cores

        if not part1_cores * part2_cores:
            raise ValueError('insufficient cores for partinioning [%d, %d]' 
                             % (part1_cores, part2_cores))

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        # Define two agent partitions within that pilot
        pd_init = {'resource'     : resource,
                   'runtime'      : 15,  # pilot runtime (min)
                   'project'      : config[resource]['project'],
                   'queue'        : config[resource]['queue'],
                   'access_schema': config[resource]['schema'],

                   'agent_cores'  : 'automatic'  # auto-add to partition sizes
                   'partitions'   : [{'config': 'aprun', 'cores' : part1_cores}, 
                                     {'config': 'orte',  'cores' : part2_cores}]
                  }
        pdesc = rp.ComputePilotDescription(pd_init)

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        print 'pilot info: %d cores (%d + %d)' \
                % (pilot.cores, pilot.partitions[0].cores, 
                                pilot.partitions[1].cores)

        report.header('submit units')

        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        # Create a workload of ComputeUnits.
        # Each compute unit runs '/bin/date'.

        n = 256  # number of units to run
        report.info('create %d unit description(s)\n\t' % n)

        cuds = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            cud.executable = '/bin/date'
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
            report.plain('  * %s [%3d - %4s] : %s @ %s\n' \
                    % (unit.uid, unit.exit_code, unit.state[:4],
                       unit.partition, unit.pilot))


        # ----------------------------------------------------------------------
        # This part of work is done - now reconfigure partition 2 to also use
        # aprun
        pilot.reconfigure({'partitions' : [{'action' : None},
                                           {'action' : 'replace',
                                            'config' : 'aprun',
                                            'cores'  : part2_cores}]})

        print 'run more units ...'

    except Exception as e:
        # Something unexpected happened in the pilot code above
        report.error('caught Exception: %s\n' % e)
        ru.print_exception_trace()
        raise
   
    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        ru.print_exception_trace()
        report.warn('exit requested\n')
 
    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        session.close(download=True)

    report.header()


#-------------------------------------------------------------------------------

