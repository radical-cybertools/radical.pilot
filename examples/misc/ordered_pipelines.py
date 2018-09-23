#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if   len(sys.argv) > 1: resource = sys.argv[1]
    else                  : resource = 'local.localhost'

    session = rp.Session()

    try:
        # read the config used for resource details
        report.info('read config')
        config = ru.read_json('%s/../config.json' % os.path.dirname(__file__))
        report.ok('>>ok\n')

        report.header('submit pilots')

        pd_init = {'resource'      : resource,
                   'runtime'       : 60,  # pilot runtime (min)
                   'exit_on_error' : True,
                   'project'       : config[resource]['project'],
                   'queue'         : config[resource]['queue'],
                   'access_schema' : config[resource]['schema'],
                   'cores'         : config[resource]['cores']
                  }
        pdesc = rp.ComputePilotDescription(pd_init)
        pmgr  = rp.PilotManager(session=session)
        pilot = pmgr.submit_pilots(pdesc)

        report.header('submit pipelines')

        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        if len(sys.argv) > 2: N = int(sys.argv[2])
        else                : N = 8

        P = N

        cuds = list()
        for p in range(P):

            S = p + 1

            for s in range(S):

                U = S
                T = 10.0 / float(p + 1)

                report.info('create %d units for pipeline %s:%d\n\t' % (U, p, s))
                for u in range(U):

                    cud = rp.ComputeUnitDescription()
                    cud.executable       = '/bin/sleep'
                    cud.arguments        = [T]
                    cud.gpu_processes    = 0
                    cud.cpu_processes    = 1
                    cud.cpu_threads      = 1
                    cud.cpu_process_type = rp.POSIX
                    cud.cpu_thread_type  = rp.POSIX
                    cud.tags             = {'order' : '%s %d %d' % (p, s, U)}
                    cud.name             =  '%s %d %d' % (p, s, u)
                    cuds.append(cud)
                    report.progress()
                report.ok('>>ok %3.1f\n' % T)

        # the agent scheduler can handle units independent of submission order
        # sort by stage
        cuds = sorted(cuds, key=lambda e: [int(x) for x in e.name.split()][1])

        import random
        random.shuffle(cuds)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')
        umgr.wait_units()


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


# ------------------------------------------------------------------------------

