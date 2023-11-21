#!/usr/bin/env python3

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
        config = ru.read_json('%s/../config.json' % pwd)
        report.ok('>>ok\n')

        report.header('submit pilots')

        pd_init = {'resource'      : resource,
                   'runtime'       : 60,  # pilot runtime (min)
                   'exit_on_error' : True,
                   'cores'         : 32
                  }
        pdesc = rp.PilotDescription(pd_init)
        pmgr  = rp.PilotManager(session=session)
        pilot = pmgr.submit_pilots(pdesc)

        report.header('submit bags of tasks')

        tmgr = rp.TaskManager(session=session)
        tmgr.add_pilots(pilot)

        # run N bags of tasks, where each bag contains M tasks of different
        # sizes.  All tasks within the same bag are to get scheduled on the
        # same node (colocated)

        n_bags    = 2
        bag_size  = 3
        task_size = [5, 1, 4]

        assert len(task_size) == bag_size

        tds = list()
        for b in range(n_bags):
            for tid,s in enumerate(task_size):
                td = rp.TaskDescription()
                td.executable = '%s/colocated_task.sh' % pwd
                td.arguments  = [b, bag_size, tid]
                td.ranks      = s
                td.tags       = {'colocate': {'bag' : b, 'size': bag_size}}
                td.name       =  'b%03d-t%03d' % (b, tid)
                print(td.name)
                tds.append(td)
                report.progress()

        random.shuffle(tds)

        tmgr.submit_tasks(tds)

        report.header('gather results')
        tmgr.wait_tasks()


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
        report.warn('exit requested with %s\n' % e)

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        session.close(download=True)

    report.header()


# ------------------------------------------------------------------------------

