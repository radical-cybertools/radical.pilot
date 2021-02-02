#!/usr/bin/env python 

import os
import sys

import threading

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def log(msg):

    sys.stdout.write('%s\n' % msg)
    sys.stdout.flush()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    uid       = os.environ['RP_TASK_ID']
    addr_work = os.environ['RP_WORK_QUEUE_IN']
    addr_res  = os.environ['RP_RESULT_QUEUE_OUT']

    n_clients = int(sys.argv[1])
    term      = threading.Event()
    term_cnt  = 0

    work_queue   = ru.Putter('RP_WORK_QUEUE',   addr_work)
    result_queue = ru.Getter('RP_RESULT_QUEUE', addr_res)

    for i in range(1000):
        log('> %06d ?' % (i))
        work_queue.put(i)

    for i in range(1000):
        result = result_queue.get()
        log('< %06d : %s' % (i, result))

    for i in range(n_clients):
        log('x %02d QUIT' % i)
        work_queue.put('QUIT')

    log('m DONE %s')
    sys.exit()


# ------------------------------------------------------------------------------

