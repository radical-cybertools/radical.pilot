#!/usr/bin/env python3

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

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
                   'project'       : config[resource]['project'],
                   'queue'         : config[resource]['queue'],
                   'access_schema' : config[resource]['schema'],
                   'cores'         : config[resource]['cores']
                  }
        pdesc = rp.PilotDescription(pd_init)
        pmgr  = rp.PilotManager(session=session)
        pilot = pmgr.submit_pilots(pdesc)

        report.header('submit pipelines')

        tmgr = rp.TaskManager(session=session)
        tmgr.add_pilots(pilot)

        if len(sys.argv) > 2: N = int(sys.argv[2])
        else                : N = 8

        n_pipes  = 2
        n_stages = 5
        n_tasks  = 4

        tds = list()
        for p in range(n_pipes):
            for s in range(n_stages):
                for t in range(n_tasks):
                    td = rp.TaskDescription()
                    td.executable       = '%s/pipeline_task.sh' % pwd
                    td.arguments        = [p, s, t, 10]
                    td.cpu_processes    = 1
                    td.tags             = {'order': {'ns'   : p,
                                                      'order': s,
                                                      'size' : n_tasks}}
                    td.name             =  'p%03d-s%03d-t%03d' % (p, s, t)
                    tds.append(td)
                    report.progress()

        import random
        random.shuffle(tds)

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        tmgr.submit_tasks(tds)

        # Wait for all tasks to reach a final state
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

