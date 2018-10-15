#!/usr/bin/env python

import zmq
import sys
import time


# ------------------------------------------------------------------------------
#
delay = 0.0

addr = None
with open('test.bridge.url', 'r') as fin:
    for line in fin.readlines():
        tag, addr = line.split()
        if tag == 'OUT':
            break

print 'add: %s' % addr

context    = zmq.Context()
socket     = context.socket(zmq.REQ)
socket.hwm = 10
socket.connect(addr)

while True:
    socket.send('request')
    tag, msg = socket.recv_multipart()
  # print '%s [%s]' % (msg, tag)
    sys.stdout.write('.')
    sys.stdout.flush()
    time.sleep (delay)




# ------------------------------------------------------------------------------

