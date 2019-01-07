#!/usr/bin/env python

import os
import zmq
import time
import msgpack


# ------------------------------------------------------------------------------
#
DELAY = 0.0

addr = None
with open('test.bridge.url', 'r') as fin:
    for line in fin.readlines():
        tag, addr = line.split()
        if tag == 'GET':
            break

print 'GET: %s' % addr

context    = zmq.Context()
socket     = context.socket(zmq.REQ)
socket.hwm = 1
socket.connect(addr)

while True:
    socket.send('request %d' % os.getpid())
    msg = msgpack.unpackb(socket.recv())
    print '<- %s' % msg
    time.sleep(DELAY)


# ------------------------------------------------------------------------------

