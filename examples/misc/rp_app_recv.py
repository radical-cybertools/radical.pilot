#!/usr/bin/env python 

import os
import sys
import zmq


def log(topic, msg):
    sys.stdout.write('recv %s: %s\n' % (topic, msg))
    sys.stdout.flush()


if __name__ == '__main__':

    addr = os.environ['APP_PUBSUB_OUT'] 
    print 'addr: %s' % addr

    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(addr)
    sub.setsockopt_string(zmq.SUBSCRIBE, u'topic')

    while True:
        topic, msg = sub.recv_multipart()
        log(topic, msg)
        if msg == 'STOP':
            break

    log(topic, msg)

