#!/usr/bin/env python 

import zmq
import time

addr = None
with open('test.bridge.url', 'r') as fin:
    for line in fin.readlines():
        tag, addr = line.split()
        if tag == 'PUB':
            break

print 'add: %s' % addr

context = zmq.Context()
socket  = context.socket(zmq.PUB)
socket.connect(addr)

n     = 300
start = time.time()
topic = 'topic'
for index in range(n):
    socket.send_multipart([topic, '-'])
    socket.send_multipart([topic, '+'])

stop = time.time()

socket.send_multipart([topic, 'x'])
print '-> %.2f /s' % ((n * 2) / (stop - start))

