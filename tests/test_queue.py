#!/usr/bin/env python 

import time
import radical.pilot.utils as rpu

N = 10000

def test():

    print "n  : %d" % N

    q_src = rpu.Queue.create (rpu.QUEUE_THREAD,  'unit_update_queue', rpu.QUEUE_SOURCE)
    q_bri = rpu.Queue.create (rpu.QUEUE_THREAD,  'unit_update_queue', rpu.QUEUE_BRIDGE)
    q_tgt = rpu.Queue.create (rpu.QUEUE_THREAD,  'unit_update_queue', rpu.QUEUE_TARGET)
    
    start=time.time()
    for i in range (N):
        q_src.put(i)
        q_tgt.get()
    stop=time.time()
    print "mtq: %.3f" % (stop-start)
    
    q_src = rpu.Queue.create (rpu.QUEUE_PROCESS, 'unit_update_queue', rpu.QUEUE_SOURCE)
    q_bri = rpu.Queue.create (rpu.QUEUE_PROCESS, 'unit_update_queue', rpu.QUEUE_BRIDGE)
    q_tgt = rpu.Queue.create (rpu.QUEUE_PROCESS, 'unit_update_queue', rpu.QUEUE_TARGET)
    
    start=time.time()
    for i in range (N):
        q_src.put(i)
        q_tgt.get()
    stop=time.time()
    print "mpq: %.3f" % (stop-start)
    
    q_src = rpu.Queue.create (rpu.QUEUE_ZMQ,     'unit_update_queue', rpu.QUEUE_SOURCE)
    q_bri = rpu.Queue.create (rpu.QUEUE_ZMQ,     'unit_update_queue', rpu.QUEUE_BRIDGE)
    q_tgt = rpu.Queue.create (rpu.QUEUE_ZMQ,     'unit_update_queue', rpu.QUEUE_TARGET)
    
    start=time.time()
    for i in range (N):
        q_src.put(i)
        q_tgt.get()
    stop=time.time()
    print "zmq: %.3f" % (stop-start)
    
    # q_bri.close()

test()

