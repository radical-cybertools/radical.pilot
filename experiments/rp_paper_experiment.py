#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys
import time

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    resource = str(sys.argv[1])
    n        = int(sys.argv[2])

    session = rp.Session()
    try:

        report.header('submit pilots')
        pd_init = {'resource'      : resource,
                   'runtime'       : 60,  # pilot runtime (min)
                   'exit_on_error' : True,
                   'project'       : 'BIP149',
                   'queue'         : 'batch',
                   'access_schema' : 'local',
                   'cores'         : n*32 + 16
                  }
        pdesc = rp.ComputePilotDescription(pd_init)
        pmgr  = rp.PilotManager(session=session)
        pilot = pmgr.submit_pilots(pdesc)

        report.header('submit units')

        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        report.info('create %d unit description(s)\n\t' % n)

        cuds = list()
        for i in range(0, n):

            cud = rp.ComputeUnitDescription()
            cud.executable       = '/lustre/atlas1/bip149/scratch/merzky1/experiments/workload.sh'
            cud.gpu_processes    = 0
            cud.cpu_processes    = 2
            cud.cpu_threads      = 16
            cud.cpu_process_type = rp.MPI  
            cud.cpu_thread_type  = rp.OpenMP
            cuds.append(cud)
            report.progress()
        report.ok('>>ok\n')

        umgr.submit_units(cuds)
        report.header('gather results')
        umgr.wait_units()


    except Exception as e:
        report.error('caught Exception: %s\n' % e)
        ru.print_exception_trace()
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        ru.print_exception_trace()
        report.warn('exit requested\n')

    finally:
        report.header('finalize')
        session.close(download=True)

    report.header()


# ------------------------------------------------------------------------------

