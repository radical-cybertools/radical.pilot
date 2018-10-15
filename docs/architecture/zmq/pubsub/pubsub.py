#!/usr/bin/env python 

import zmq

context = zmq.Context()

# Socket facing publishers
socket_in = context.socket(zmq.XSUB)
socket_in.bind("tcp://*:*")

# Socket facing Subscribers
socket_out = context.socket(zmq.XPUB)
socket_out.bind("tcp://*:*")

addr_in  = socket_in .getsockopt(zmq.LAST_ENDPOINT)
addr_out = socket_out.getsockopt(zmq.LAST_ENDPOINT)

print 'IN : %s' % addr_in
print 'OUT: %s' % addr_out

with open('test.bridge.url', 'w') as fout:
    fout.write('IN  %s\n' % addr_in)
    fout.write('OUT %s\n' % addr_out)

zmq.proxy(socket_in, socket_out)

# We never get here...
socket_out.close()
socket_in.close()
context.term()

