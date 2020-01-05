#!/usr/bin/env python3

import sys
import zmq
import time
import msgpack

import threading as mt

import radical.utils as ru


delay    = 0.0
topic    = b'topic'
nthreads = 5
hwm      = 0


def out(data):
    sys.stdout.write('%.2f  %s\n' % (time.time(), data))
    sys.stdout.flush()



def subscribe(uid):

    context        = zmq.Context()
    socket_sub     = context.socket(zmq.SUB)
    socket_sub.hwm = hwm
    socket_sub.connect("tcp://127.0.0.1:5001")

    socket_sub.setsockopt(zmq.SUBSCRIBE, ru.as_bytes('topic'))

    cnt = 0
    while True:

        msg = socket_sub.recv()
        topic, msg = msg.split(b' ', 1)
        out('%3d %7d %s | %s' % (uid, cnt, ru.as_string(topic), msgpack.unpackb(msg)))
        cnt += 1

        if delay:
            time.sleep (delay)


threads = [mt.Thread(target=subscribe, args=[n]) for n in range(nthreads)]

for thread in threads:
    thread.daemon = True
    thread.start()

for thread in threads:
    thread.join()

time.sleep(1)


