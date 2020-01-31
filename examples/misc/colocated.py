#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys
import random

import radical.pilot as rp
import radical.utils as ru

pwd = os.path.abspath(os.path.dirname(__file__))


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
                   'cores'         : 32
                  }
        pdesc = rp.ComputePilotDescription(pd_init)
        pmgr  = rp.PilotManager(session=session)
        pilot = pmgr.submit_pilots(pdesc)

        report.header('submit bags of tasks')

        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        # run N bags of tasks, where each bag contains M tasks of different
        # sizes.  All tasks within the same bag are to get scheduled on the
        # same node (colocated)

        n_bags    = 2
        bag_size  = 3
        task_size = [5, 1, 4]

        assert(len(task_size) == bag_size)

        cuds = list()
        for b in range(n_bags):
            for tid,s in enumerate(task_size):
                cud = rp.ComputeUnitDescription()
                cud.executable       = '%s/colocated_task.sh' % pwd
                cud.arguments        = [b, bag_size, tid]
                cud.cpu_processes    = s
                cud.cpu_process_type = rp.MPI
                cud.tags             = {'colocate': {'bag' : b,
                                                     'size': bag_size}}
                cud.name             =  'b%03d-t%03d' % (b, tid)
                print(cud.name)
                cuds.append(cud)
                report.progress()

        random.shuffle(cuds)

        umgr.submit_units(cuds)

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

