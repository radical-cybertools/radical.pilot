#!/usr/bin/env python

import os
import csv
import time
import threading

AGENT_THREADS  = 'threads'
AGENT_MODE     = AGENT_THREADS
profile_agent  = True


# ------------------------------------------------------------------------------
#
timestamp_zero = float(os.environ.get('TIME_ZERO', time.time()))
def timestamp_now():
    # relative timestamp seconds since TIME_ZERO (start)
    return float(time.time()) - timestamp_zero


# ------------------------------------------------------------------------------
#
csvfile   = open('test_profile_writer.csv', 'wb')
csvwriter = csv.writer (csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
def prof_csv(etype, uid="", msg="", logger=None):

    if logger:
        logger("%s (%10s) : %s", etype, msg, uid)

    if not profile_agent:
        return

    now = timestamp_now()

    # TODO: Layer violation?
    if   AGENT_MODE == AGENT_THREADS  : tid = threading.current_thread().name
    elif AGENT_MODE == AGENT_PROCESSES: tid = os.getpid()
    else: raise Exception('Unknown Agent Mode')

    csvwriter.writerow([now, tid, uid, etype, msg])

# ------------------------------------------------------------------------------
#
profile_handle = open('test_profile_writer.prof', 'wb')
def prof_write(etype, uid="", msg="", logger=None):

    if logger:
        logger("%s (%10s) : %s", etype, msg, uid)

    if not profile_agent:
        return

    now = timestamp_now()

    # TODO: Layer violation?
    if   AGENT_MODE == AGENT_THREADS  : tid = threading.current_thread().name
    elif AGENT_MODE == AGENT_PROCESSES: tid = os.getpid()
    else: raise Exception('Unknown Agent Mode')

    profile_handle.write(" %12.4f : %-17s : %-24s : %-40s : %s\n" \
                         % (now, tid, uid, etype, msg))

    # FIXME: disable flush on production runs
    #profile_handle.flush()

# ------------------------------------------------------------------------------
#
NUM = 1000 * 1000

start = time.time()
for i in range(NUM):
    prof_write (str(i), str(i*i))
stop = time.time()
print 'write: %f' % (stop-start)

start = time.time()
for i in range(NUM):
    prof_csv (str(i), str(i*i))
stop = time.time()
print 'write: %f' % (stop-start)

os.system ('rm -f test_profile_writer.prof')
os.system ('rm -f test_profile_writer.csv')

