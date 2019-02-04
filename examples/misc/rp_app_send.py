#!/usr/bin/env python 

import os
import sys
import zmq
import time


def log(topic, msg):
    sys.stdout.write('send %s: %s\n' % (topic, msg))
    sys.stdout.flush()


if __name__ == '__main__':

    addr = os.environ['APP_PUBSUB_IN'] 
    print 'addr: %s' % addr

    context = zmq.Context()
    socket  = context.socket(zmq.PUB)
    socket.connect(addr)

    n     = 300
    topic = 'topic'
    for i in range(n):
        msg = '%d' % i
        socket.send_multipart([topic, msg])
        log(topic, msg)
        sys.stdout.flush()
        time.sleep(0.01)

    log(topic, 'STOP')
    socket.send_multipart([topic, 'STOP'])

