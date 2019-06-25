#!/usr/bin/env python 

import os
import sys

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def log(msg):

    sys.stdout.write('%s\n' % msg)
    sys.stdout.flush()


# ------------------------------------------------------------------------------
#
# WORKER
#
if __name__ == '__main__':

    uid          = os.environ['RP_UNIT_ID']
    addr_work    = os.environ['RP_WORK_QUEUE_OUT']
    addr_res     = os.environ['RP_RESULT_QUEUE_IN']

    work_queue   = ru.Getter('RP_WORK_QUEUE',   addr_work)
    result_queue = ru.Putter('RP_RESULT_QUEUE', addr_res)

    work = work_queue.get()
    while work:

        result = '%s: %s' % (uid, work)
        log('= %s' % result)
        result_queue.put(result)
        work = work_queue.get()

        if work == ['QUIT']:
            break

    log('w DONE %s' % uid)


# ------------------------------------------------------------------------------

