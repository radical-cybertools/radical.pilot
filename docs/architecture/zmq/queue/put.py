#!/usr/bin/env python

import zmq
import time
import msgpack

DELAY = 0.5


# ------------------------------------------------------------------------------
#
addr = None
with open('test.bridge.url', 'r') as fin:
    for line in fin.readlines():
        tag, addr = line.split()
        if tag == 'PUT':
            break

print 'PUT: %s' % addr

context    = zmq.Context()
socket     = context.socket(zmq.PUSH)
socket.hwm = 1
socket.connect(addr)

for n in xrange(1000):
    msg = {'data' : n}
    socket.send(msgpack.packb(msg))
    print '-> %s' % msg
    time.sleep(DELAY)


# ------------------------------------------------------------------------------

