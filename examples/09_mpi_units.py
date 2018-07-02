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

PWD = os.path.abspath(os.path.dirname(__file__))
helloworld_mpi_bin  = 'helloworld_mpi.py'
helloworld_mpi_path = '%s/%s' % (PWD, helloworld_mpi_bin)


# -----------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
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
        config = ru.read_json('%s/config.json' % PWD)
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
                   'cores'         : config[resource]['cores'],
                  }
        pdesc = rp.ComputePilotDescription(pd_init)

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)
        report.header('submit units')

        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        # Create a workload of ComputeUnits. 
        # Each compute unit runs a MPI test application.

        n = 2   # number of units to run
        t_num = 1  # number of threads   (OpenMP)
        p_num = 3  # number of processes (MPI)
        report.info('create %d unit description(s)\n\t' % n)

        cuds = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            cud.executable     = '/bin/sh'
            cud.arguments      = ['09_mpi_units.sh']
            cud.input_staging  = ['%s/09_mpi_units.sh' % PWD]
            cud.cores          = 3
            cud.mpi            = True
            cuds.append(cud)
            report.progress()
        report.ok('>>ok\n')

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cuds)

        # Wait for all units to reach a final state (DONE, CANCELED or FAILED)
        report.header('gather results')
        umgr.wait_units()

        report.info('\n')
        for unit in units:
            report.plain('  * %s: %s, exit: %3s, ranks: %s\n'
                    % (unit.uid, unit.state[:4], unit.exit_code, unit.stdout))
            ranks = list()
            for line in unit.stdout.split('\n'):
                if line.strip():
                    rank = line.split()[1]
                    ranks.append(rank)
            for p in range(p_num):
                for t in range(t_num):
                    rank = '%d:%d/1' % (p, t)
                    assert(rank in ranks), 'missing rank %s' % rank


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


# ------------------------------------------------------------------------------

