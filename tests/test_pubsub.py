#!/usr/bin/env python 

import os
import zmq
import time
import copy
import json
import random
import Queue
import threading           as mt
import multiprocessing     as mp
import radical.utils       as ru
import radical.pilot.utils as rpu


def pub():
    ctx   = zmq.Context()
    q     = ctx.socket(zmq.PUB)
    q.connect('tcp://127.0.0.1:%d' % 30000)

    i=0
    while True:
        i+=1
        q.send_multipart(["state", json.dumps({"local" : "%d" % i})])
        time.sleep(1)


def bridge():

    ctx     = zmq.Context()

    _in      = ctx.socket(zmq.XSUB)
    _in.bind('tcp://*:%d' % 30000)

    _out      = ctx.socket(zmq.XPUB)
    _out.bind('tcp://*:%d' % 30001)
    
    _poll = zmq.Poller()
    _poll.register(_in,  zmq.POLLIN)
    _poll.register(_out, zmq.POLLIN)

    while True:

        events = dict(_poll.poll(1000)) # timeout in ms

        if _in in events:
            msg = _in.recv_multipart()
            _out.send_multipart(msg)
            print "-> %s" % msg

        if _out in events:
            msg = _out.recv_multipart()
            _in.send_multipart(msg)
            print "<- %s" % msg

def sub():

    ctx   = zmq.Context()
    q     = ctx.socket(zmq.SUB)
  # q.hwm = 1
    q.connect('tcp://localhost:%d' % 30001)
    q.setsockopt(zmq.SUBSCRIBE, "state")

    while True:
        print '<-%s' % q.recv_multipart()


# b = mt.Thread(target=bridge)
# b.start()

# s = mt.Thread(target=sub)
# s.start()

b = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'ps', rpu.PUBSUB_BRIDGE, 'tcp://127.0.0.1:30000')

s = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'ps', rpu.PUBSUB_SUB,    'tcp://127.0.0.1:30000')
s.subscribe('state')

p2 = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'ps', rpu.PUBSUB_PUB,    'tcp://127.0.0.1:30000')

time.sleep (1)

p2.put('state', {'hey' : 'rpu'})
print "---------"

while True:
    print "<= %s" % s.get()

