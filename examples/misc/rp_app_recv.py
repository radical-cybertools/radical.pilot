#!/usr/bin/env python 

import os
import sys

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def log(uid, msg, i):

    sys.stdout.write('recv %s: %s [%5d]\n' % (uid, str(msg)[:128], i))
    sys.stdout.flush()


# ------------------------------------------------------------------------------
#
# WORKER
#
if __name__ == '__main__':

    uid          = os.environ['RP_UNIT_ID']
    addr_work    = os.environ['APP_QUEUE_OUT']
    addr_res     = os.environ['APP_PUBSUB_IN']

    work_queue   = ru.Getter('WORK_QUEUE',   addr_work)
    result_queue = ru.Putter('RESULT_QUEUE', addr_res)

    work = work_queue.get()
    while work:

        result = len(work)
        result_queue.put(result)
        work = work_queue.get()


# ------------------------------------------------------------------------------

