#!/usr/bin/env python

import os
import sys
import time
import socket
import threading       as mt
import multiprocessing as mp

threads = list()
procs   = list()
files   = list()
sockets = list()


def _work():
    time.sleep(10)


print
while True:
    try:
        t = mt.Thread(target=_work)
        t.start()
        threads.append(t)
    except:
        break

print 'threads: %5d' % len(threads)
for t in threads:
    t.join()


print
while True:
    try:
        p = mp.Process(target=_work)
        p.start()
        procs.append(p)
    except:
        break

print 'procs  : %5d' % len(procs)
for p in procs:
    p.kill()
    p.join()


print
base = '/tmp/rp_limit_%d.%%d' % os.getpid()
while True:
    try:
        f = open(base % len(files), 'w')
        files.append(f)
    except:
        break

print 'files  : %5d' % len(files)
for f in files:
    os.unlink(f.name)
    f.close()

print
host = 'localhost'
port = 22
while True:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect([host, port])
        sockets.append(s)
    except:
        break

print 'sockets: %5d' % len(sockets)
for s in sockets:
    s.close()

print
