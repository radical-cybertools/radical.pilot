#!/usr/bin/env python

import sys
import zmq
import time

name  = 'put'
delay = 0.0


# ------------------------------------------------------------------------------
#
addr = None
with open('test.bridge.url', 'r') as fin:
    for line in fin.readlines():
        tag, addr = line.split()
        if tag == 'IN':
            break

print 'add: %s' % addr

context    = zmq.Context()
socket     = context.socket(zmq.PUSH)
socket.hwm = 10
socket.connect(addr)

for num in xrange(1000):
    socket.send_multipart([name, str(num)])
  # print '-> %s [%s]' % (num, name)
    time.sleep(delay)


# ------------------------------------------------------------------------------

