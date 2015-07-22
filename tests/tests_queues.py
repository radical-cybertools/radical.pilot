#!/usr/bin/env python 

import radical.pilot.utils as rpu

def test():
    q_src = rpu.RPUQueue.create (rpu.QUEUE_THREAD,  'unit_update_queue', rpu.QUEUE_SOURCE)
    q_bri = rpu.RPUQueue.create (rpu.QUEUE_THREAD,  'unit_update_queue', rpu.QUEUE_BRIDGE)
    q_tgt = rpu.RPUQueue.create (rpu.QUEUE_THREAD,  'unit_update_queue', rpu.QUEUE_TARGET)
    
    for i in range (1000):
        q_src.put(i)
        print q_tgt.get(),
    
    q_src = rpu.RPUQueue.create (rpu.QUEUE_PROCESS, 'unit_update_queue', rpu.QUEUE_SOURCE)
    q_bri = rpu.RPUQueue.create (rpu.QUEUE_PROCESS, 'unit_update_queue', rpu.QUEUE_BRIDGE)
    q_tgt = rpu.RPUQueue.create (rpu.QUEUE_PROCESS, 'unit_update_queue', rpu.QUEUE_TARGET)
    
    for i in range (1000):
        q_src.put(i)
        print q_tgt.get(),
    
    q_src = rpu.RPUQueue.create (rpu.QUEUE_REMOTE,  'unit_update_queue', rpu.QUEUE_SOURCE)
    q_bri = rpu.RPUQueue.create (rpu.QUEUE_REMOTE,  'unit_update_queue', rpu.QUEUE_BRIDGE)
    q_tgt = rpu.RPUQueue.create (rpu.QUEUE_REMOTE,  'unit_update_queue', rpu.QUEUE_TARGET)
    
    for i in range (1000):
        q_src.put(i)
        print q_tgt.get(),
    
    # q_bri.close()

test()

