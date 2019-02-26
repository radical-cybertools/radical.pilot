#!/usr/bin/env python 

import os
import sys
import time

import threading

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def log(uid, msg):

    sys.stdout.write('send %s: %s\n' % (uid, str(msg)[:128]))
    sys.stdout.flush()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    uid       = os.environ['RP_UNIT_ID']
    addr_work = os.environ['APP_QUEUE_IN']
    addr_res  = os.environ['APP_PUBSUB_OUT']

    n_master  = int(sys.argv[1])
    term      = threading.Event()
    term_cnt  = 0


    work_queue   = ru.Putter('WORK_QUEUE',   addr_work)
    result_queue = ru.Getter('RESULT_QUEUE', addr_res)

    for i in range(1000):
        work_queue.put(str(i))

    for _ in range(1000):
        result = result_queue.get()
        print result

    log(uid, 'DONE')
    sys.exit()


# ------------------------------------------------------------------------------

