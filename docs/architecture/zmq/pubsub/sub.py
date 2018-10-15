#!/usr/bin/env python 

import sys
import zmq
import time

addr = None
with open('test.bridge.url', 'r') as fin:
    for line in fin.readlines():
        tag, addr = line.split()
        if tag == 'OUT':
            break

print 'add: %s' % addr

ctx = zmq.Context()
sub = ctx.socket(zmq.SUB)
sub.connect(addr)
sub.setsockopt_string(zmq.SUBSCRIBE, u'topic_1')

n = 0
start = time.time()
while True:
    top, msg = sub.recv_multipart()
    sys.stdout.write(msg)
    sys.stdout.flush()
    n += 1 
    if msg == 'x':
        break
print
stop = time.time()
print '<- %.2f /s' % (n / (stop - start))

