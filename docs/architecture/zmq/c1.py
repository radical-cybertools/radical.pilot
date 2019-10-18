#!/usr/bin/env python

import sys
import zmq
import time

name  = sys.argv[1]
delay = sys.argv[2]


def producer():
    context        = zmq.Context()
    socket_src     = context.socket(zmq.PUSH)
    socket_src.hwm = 10
    socket_src.connect("tcp://127.0.0.1:5000")

    for num in xrange(1000):
        msg = {name:num}
        socket_src.send_json(msg)
        print('sent %s' % msg)
        time.sleep (float(delay))


producer()

