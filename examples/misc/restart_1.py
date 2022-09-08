#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

import radical.pilot as rp
import radical.utils as ru


pwd = os.path.dirname(os.path.abspath(__file__))
dh  = ru.DebugHelper()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # die - this is where the restart attempt will continue
    # but before, we store some UIDs
    with ru.ru_open('%s/restart.dat' % pwd, 'r') as fin:
        session_id = fin.readline().split()[1].strip()
        tmgr_id    = fin.readline().split()[1].strip()
        pmgr_id    = fin.readline().split()[1].strip()
        pilot_id   = fin.readline().split()[1].strip()
        task_ids   = fin.readline().split()[1].strip().split(':')

    print('session_id : %s' % session_id)
    print('tmgr_id    : %s' % tmgr_id)
    print('pmgr_id    : %s' % pmgr_id)
    print('pilot_id   : %s' % pilot_id)
    print('task_ids   : %s' % task_ids)

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: resource = sys.argv[1]
    else                   : resource = 'local.localhost'

    try:
        # Create a new session, and reconnect tmgr and pmgr
        session = rp.Session(uid=session_id)
        tmgr    = rp.TaskManager(session=session,  uid=tmgr_id)
        pmgr    = rp.PilotManager(session=session, uid=pmgr_id)

        # re-add pilots to the uymgr
        tmgr.add_pilots(pmgr.get_pilots())

        print(pmgr.list_pilots())
        print(tmgr.list_tasks())

        for task in tmgr.get_tasks():
            print(task.uid, task.state)


        # Wait for all compute tasks to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')
        tmgr.wait_tasks()


    except Exception as e:
        # Something unexpected happened in the pilot code above
        report.error('caught Exception: %s\n' % e)
        ru.print_exception_trace()
        raise

    except (KeyboardInterrupt, SystemExit):
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

