#!/usr/bin/env python

import sys
import time
import radical.pilot.utils as rpu

def test_queue():

   # FIXME: test disabled
   return


   N = 1000


   def test():

       print "n  : %d" % N

       q_in  = rpu.Queue.create (rpu.QUEUE_THREAD,  'ping_queue', rpu.QUEUE_INPUT)
       q_bri = rpu.Queue.create (rpu.QUEUE_THREAD,  'ping_queue', rpu.QUEUE_BRIDGE)
       q_out = rpu.Queue.create (rpu.QUEUE_THREAD,  'ping_queue', rpu.QUEUE_OUTPUT)

       start = time.time()
       for i in range (N):
           q_in .put(i)
           q_out.get()
       stop = time.time()
       diff = stop - start
       print "mtq : %4.2f (%8.1f)" % (diff, N / diff)

       q_in  = rpu.Queue.create (rpu.QUEUE_PROCESS, 'ping_queue', rpu.QUEUE_INPUT)
       q_bri = rpu.Queue.create (rpu.QUEUE_PROCESS, 'ping_queue', rpu.QUEUE_BRIDGE)
       q_out = rpu.Queue.create (rpu.QUEUE_PROCESS, 'ping_queue', rpu.QUEUE_OUTPUT)

       start = time.time()
       for i in range (N):
           q_in .put(i)
           q_out.get()
       stop = time.time()
       diff = stop - start
       print "mtp : %4.2f (%8.1f)" % (diff, N /diff)

       q_in  = rpu.Queue.create (rpu.QUEUE_ZMQ,     'ping_queue', rpu.QUEUE_INPUT)
       q_bri = rpu.Queue.create (rpu.QUEUE_ZMQ,     'ping_queue', rpu.QUEUE_BRIDGE)
       q_out = rpu.Queue.create (rpu.QUEUE_ZMQ,     'ping_queue', rpu.QUEUE_OUTPUT)

       start = time.time()
       for i in range (N):
           q_in .put(i)
           q_out.get()
       stop = time.time()
       diff = stop - start
       print "zmq : %4.2f (%8.1f)" % (diff, N / diff)

       q_bri.close()


