#!/usr/bin/env python

import os
import sys
import zmq
import time
import multiprocessing as mp

cfg = {
    'src'     : {
        'n'   : 1000,
        't'   : 0.1
        },
    'queue'   : {
        't'   : 0.1
        },
    'tgt'     : {
        't'   : 0.1
        }
    }

def src(host):
    context        = zmq.Context()
    socket_src     = context.socket(zmq.PUSH)
    socket_src.hwm = 10
    socket_src.connect("tcp://%s:5000" % host)

    n = cfg['src']['n']
    t = cfg['src']['t']

    pid = os.getpid()

    for num in range(n):
        msg = {pid:num}
        socket_src.send_json(msg)
        print 'sent %s' % msg
        time.sleep (t)


def queue(host):
    context        = zmq.Context()
    socket_src     = context.socket(zmq.PULL)
    socket_src.hwm = 10
    socket_src.bind("tcp://%s:5000" % host)

    context         = zmq.Context()
    socket_sink     = context.socket(zmq.REP)
    socket_sink.hwm = 10
    socket_sink.bind("tcp://127.0.0.1:5001")

    t = cfg['queue']['t']

    while True:
        req = socket_sink.recv()
        msg = socket_src.recv_json()
        print msg
        socket_sink.send_json(msg)

        if t:
            time.sleep (t)


def tgt(host):
    context         = zmq.Context()
    socket_sink     = context.socket(zmq.REQ)
    socket_sink.hwm = 10
    socket_sink.connect("tcp://127.0.0.1:5001")

    t = cfg['tgt']['t']
    
    while True:
        socket_sink.send('request')
        msg = socket_sink.recv_json()
        print 'got %s' % msg
        time.sleep (t)

if len(sys.argv) < 3:
    print """

    usage: %s <host> <type> [<type>]

    """
    sys.exit(-1)


host = sys.argv[1]

if host in ['local', 'localhost']:
    host = '127.0.0.1'

procs = list()
for arg in sys.argv[2:]:
    if arg == 'src'  : procs.append (mp.Process(target=src  , args=[host]))
    if arg == 'queue': procs.append (mp.Process(target=queue, args=[host]))
    if arg == 'tgt'  : procs.append (mp.Process(target=tgt  , args=[host]))

# Run processes
for p in procs:
    p.start()

# Exit the completed processes
for p in procs:
    p.join()

        
