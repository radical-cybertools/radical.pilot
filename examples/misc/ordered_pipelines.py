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
    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: resource = sys.argv[1]
    else                   : resource = 'local.localhost'

    session = rp.Session()

    try:
        # read the config used for resource details
        report.info('read config')
        config = ru.read_json('%s/../config.json' % os.path.dirname(__file__))
        report.ok('>>ok\n')

        report.header('submit pilots')

        pd_init = {'resource'      : resource,
                   'runtime'       : 15,  # pilot runtime (min)
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


        N = 10          # number of units per pipeline stage
        S = 4           # number of stages per pipeline
        P = {'p.1': 2,  # runtime per unit per pipeline
             'p.2': 5}

        # the pipeline ID serves as ordering namespace
        # the stage    ID serves as ordering sequencer

        cuds = list()

        for p in P:

            for s in range(S):

                report.info('create %d unit for pipeline %s:%d\n\t' % (N, p, s))
                for n in range(N):

                    cud = rp.ComputeUnitDescription()
                    cud.executable       = '/bin/sleep'
                    cud.arguments        = [P[p]]
                    cud.gpu_processes    = 0
                    cud.cpu_processes    = 1
                    cud.cpu_threads      = 1
                    cud.cpu_process_type = rp.POSIX
                    cud.cpu_thread_type  = rp.POSIX
                    cud.tags             = {'order' : '%s %d %d' % (p, s, N)}
                    cud.name             =  '%s %d %d' % (p, s, N)
                    cuds.append(cud)
                    report.progress()
                report.ok('>>ok\n')

        # the agent scheduler can handle units independent of submission order
      # import random
      # random.shuffle(cuds)

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

