#!/usr/bin/env python3

import sys
import zmq
import time
import msgpack

import radical.utils as ru

topic = 'topic'.replace(' ', '_')
delay = 0.0
hwm   = 0


def out(data):
    sys.stdout.write('%.2f  %s\n' % (time.time(), data))
    sys.stdout.flush()



def bridge():

    context = zmq.Context()
    poller  = zmq.Poller()

    socket_pub     = context.socket(zmq.XSUB)
    socket_pub.hwm = hwm

    socket_sub     = context.socket(zmq.XPUB)
    socket_sub.hwm = hwm

    socket_pub.bind("tcp://*:5000")
    socket_sub.bind("tcp://*:5001")

    poller.register(socket_pub, flags=zmq.POLLIN)
    poller.register(socket_sub, flags=zmq.POLLIN)

    cnt = 0
    while True:

        events = poller.poll(timeout=delay)

        for sock, flag in events:

            assert(flag == zmq.POLLIN)

            if sock == socket_sub:

                # if the sub socket signals a message, it's likely a topic
                # subscription.  Forward that to the pub channel, so the
                # bridge subscribes for the respective message topic.
                msg = socket_sub.recv()
                socket_pub.send(msg)

            elif sock == socket_pub:

                # this is a published message - forward to subcribers
                raw = socket_pub.recv()
                socket_sub.send(raw)
                topic, msg = raw.split(b' ', 1)
                out('  0 %7d %s | %s' % (cnt, ru.as_string(topic), msgpack.unpackb(msg)))
                cnt += 1

            else:

                assert(False), sock

        if delay:
            time.sleep (delay)


bridge()

