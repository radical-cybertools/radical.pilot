#!/usr/bin/env python3

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys
import time

os.environ['RADICAL_PILOT_VERBOSE'] = 'REPORT'

import radical.pilot as rp
import radical.utils as ru

# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: https://radicalpilot.readthedocs.io/
#
# ------------------------------------------------------------------------------

n_pilots =    100
n_tasks  =   1000
n_done   =      0


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    start = time.time()

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

        # Add a Pilot Manager. Pilot managers manage one or more Pilots.
        pmgr = rp.PilotManager(session=session)

        scheduler = rp.tmgr.scheduler.SCHEDULER_BACKFILLING
      # scheduler = rp.tmgr.scheduler.SCHEDULER_ROUND_ROBIN
      # scheduler = None

        # Register the Pilot in a TaskManager object.
        tmgr = rp.TaskManager(session=session, scheduler=scheduler)
        def task_cb(task, state):
            if state in rp.FINAL:
                global n_done
                n_done += 1
                print('%5s: %4d - %4d - %5.1f - %s' % (state[0:5], n_done,
                      n_tasks, time.time() - start, task.pilot))
          # if state in [rp.FAILED]:
          #     session.close()
        tmgr.register_callback(task_cb)

        pdescs = list()
        for resource in resources:

            # Define an [n]-core local pilot that runs for [x] minutes
            # Here we use a dict to initialize the description object
            for i in range(n_pilots):

                ch = None
                if 'osg' in resource:
                    ch = [
                        # '(HAS_MODULES =?= TRUE)',
                        # '~(HAS_CVMFS_oasis_opensciencegrid_org =?= TRUE)',
                          '!(FIU_HPCOSG_CE)',  # no network
                          '!(CIT_CMS_T2)',     # no network
                         ]
                pd_init = {
                           'resource'        : resource,
                           'runtime'         : 300,   # pilot runtime (min)
                           'exit_on_error'   : False,
                           'cores'           : config[resource].get('cores', 1),
                           'gpus'            : config[resource].get('gpus', 0),
                           'project'         : config[resource].get('project', None),
                           'queue'           : config[resource].get('queue', None),
                           'access_schema'   : config[resource].get('schema', None),
                           'cleanup'         : False,
                           'candidate_hosts' : ch
                          }
                pdesc = rp.PilotDescription(pd_init)
                pdescs.append(pdesc)

        # Launch the pilot.
        pilots = pmgr.submit_pilots(pdescs)
        tmgr.add_pilots(pilots)

        def pilot_cb(pilot, state):
            print('pilot: %s - %s - %5.1f' % (pilot.uid, state,
                                              time.time() - start))
          # if state in rp.FINAL:
          #     sys.exit()
        for pilot in pilots:
            pilot.register_callback(pilot_cb)

      # pmgr.wait_pilots(state=rp.ACTIVE)
      # sys.exit()


        # Create a workload of Tasks.
        # Each task runs '/bin/date'.
        report.header('submit tasks')

        report.info('create %d task description(s)\n\t' % n_tasks)

        tds  = list()
        start = time.time()
        for i in range(n_tasks):

            # create a new Task description, and fill it.
            # Here we don't use dict initialization.
            td = rp.TaskDescription()
            # trigger an error now and then
          # if i % 2:
            td.executable = '/bin/echo'
            td.arguments  = ['$RP_PILOT_ID']

            tds.append(td)
            report.progress()
        report.ok('>>ok\n')

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        start  = time.time()
        tasks  = tmgr.submit_tasks(tds)
        stop   = time.time()
        print(' === > %s' % (stop - start))

        # Wait for all tasks to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')
        tmgr.wait_tasks()

        report.info('\n')
        for task in tasks:
            report.plain('  * %s: %s, %5s'
                        % (task.uid, task.state[:4], task.pilot))
            if task.state in [rp.DONE]:
                report.ok('>>ok\n')
            elif task.state in [rp.FAILED]:
                report.error('>>err\n')
            else:
                report.warn('>>nok\n')


      #     if task.state in [rp.FAILED, rp.CANCELED]:
      #         report.plain('  * %s: %s, exit: %5s, err: %35s' \
      #                 % (task.uid, task.state[:4],
      #                    task.exit_code, task.stderr.strip()))
      #         report.error('>>err\n')
      #     else:
      #         report.plain('  * %s: %s, exit: %5s, out: %35s' \
      #                 % (task.uid, task.state[:4],
      #                     task.exit_code, task.stdout.strip()))
      #         report.ok('>>ok\n')


    except Exception as e:
        # Something unexpected happened in the pilot code above
        # session._log.exception('oops')
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
        report.header()
        if session:
            session.close(cleanup=False)

    report.header()


# ------------------------------------------------------------------------------

