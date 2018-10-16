#!/usr/bin/env python

import zmq
import time
import msgpack

# ------------------------------------------------------------------------------
#
delay = 0.0

# src side is proper push/pull (queue)
context       = zmq.Context()
socket_in     = context.socket(zmq.PULL)
socket_in.hwm = 1
socket_in.bind("tcp://*:*")

# out side is req/resp for load balancing
context        = zmq.Context()
socket_out     = context.socket(zmq.REP)
socket_out.hwm = 1
socket_out.bind("tcp://*:*")

addr_in  = socket_in .getsockopt(zmq.LAST_ENDPOINT)
addr_out = socket_out.getsockopt(zmq.LAST_ENDPOINT)

print 'PUT: %s' % addr_in
print 'GET: %s' % addr_out

with open('test.bridge.url', 'w') as fout:
    fout.write('PUT %s\n' % addr_in)
    fout.write('GET %s\n' % addr_out)

# zmq.proxy(socket_in, socket_out)

while True:
    # only read from socket_in once we have a consumer requesting the data on
    # socket_out.
    # TODO: For multiple sinks, make sure we send replies to the right one
    req = socket_out.recv()
    msg = msgpack.unpackb(socket_in.recv())
    msg['req'] = req
    print '<> %s' % msg
    socket_out.send(msgpack.packb(msg))

    time.sleep(delay)


# ------------------------------------------------------------------------------

