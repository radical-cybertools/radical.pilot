#!/usr/bin/env python3

import sys
import zmq
import time
import msgpack

import threading as mt


name     = 'foo'
delay    =  0
topic    = 'topic'.replace(' ', '_')
hwm      = 0  # otherwise messages get lost on fast send
nthreads = 3
nmsgs    = 100000
mod      = 1


def out(data):
    sys.stdout.write('%.2f  %s\n' % (time.time(), data))
    sys.stdout.flush()


def publish(uid):

    context        = zmq.Context().instance()
    socket_pub     = context.socket(zmq.PUB)
    socket_pub.hwm = hwm
    socket_pub.connect("tcp://127.0.0.1:5000")

    time.sleep(0.1)

    for cnt in range(nmsgs):

        msg  = {b'uid': uid,
                b'cnt': cnt}

        data = bytes(topic, 'utf-8') + b' ' + msgpack.packb(msg)
        socket_pub.send(data, flags=zmq.NOBLOCK)

        if not cnt % mod:
            out('%3d %7d %s | %s' % (uid, cnt, topic, msg))

        if delay:
            time.sleep(delay)


threads = [mt.Thread(target=publish, args=[n]) for n in range(nthreads)]

for thread in threads:
  # thread.daemon = True
    thread.start()

for thread in threads:
    thread.join()

