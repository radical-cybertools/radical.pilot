#!/usr/bin/env python

import zmq
import sys
import time

t = float(sys.argv[1])


def sink():
    context         = zmq.Context()
    socket_sink     = context.socket(zmq.REQ)
    socket_sink.hwm = 10
    socket_sink.connect("tcp://127.0.0.1:5001")

    while True:
        socket_sink.send(b'request')
        msg = socket_sink.recv_json()
        print('got %s' % msg)
        time.sleep (t)


sink()

