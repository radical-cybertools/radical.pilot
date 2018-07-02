#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

import radical.pilot as rp
import radical.utils as ru

PWD = os.path.dirname(os.path.abspath(__file__))

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
        config = ru.read_json('%s/../config.json' % PWD)
        report.ok('>>ok\n')

        report.header('submit pilots')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pd_init = {
                      'resource'      : resource,
                      'runtime'       : 15,  # pilot runtime (min)
                      'exit_on_error' : True,
                      'project'       : config[resource]['project'],
                      'queue'         : config[resource]['queue'],
                      'access_schema' : config[resource]['schema'],
                      'cores'         : 16*10,
                      'gpus'          : config[resource]['gpus'],
                  }
        pdesc = rp.ComputePilotDescription(pd_init)

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)


        report.header('submit units')

        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        # Create a workload of ComputeUnits.
        # Each compute unit runs '/bin/date'.

        n = 2   # number of units to run
        report.info('create %d unit description(s)\n\t' % n)

        cuds = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            cud.executable       = '/bin/sh'
            cud.arguments        = ['/lustre/atlas/scratch/merzky1/csc230/radical.pilot.sandbox/09_mpi_units.sh']
            cud.cpu_processes    = 2
            cud.cpu_threads      = 1
          # cud.cpu_process_type = rp.MPI
            cud.cpu_thread_type  = rp.OpenMP
            cud.gpu_processes    = 1
            cud.gpu_threads      = 1
          # cud.gpu_process_type = rp.MPI
            cud.gpu_thread_type  = rp.OpenMP
            cuds.append(cud)
            report.progress()

        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            cud.executable       = '/bin/sh'
            cud.arguments        = ['/lustre/atlas/scratch/merzky1/csc230/radical.pilot.sandbox/09_mpi_units.sh']
            cud.cpu_processes    = 0
            cud.cpu_threads      = 1
            cud.cpu_process_type = rp.MPI
            cud.cpu_thread_type  = rp.OpenMP
            cud.gpu_processes    = 4
            cud.gpu_threads      = 1
            cud.gpu_process_type = rp.MPI
            cud.gpu_thread_type  = rp.OpenMP
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

        for unit in units:
            report.plain('  * %s: %s, exit: %3s, out: %s\n'
                    % (unit.uid, unit.state[:4], unit.exit_code, unit.stdout))


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


# ------------------------------------------------------------------------------

