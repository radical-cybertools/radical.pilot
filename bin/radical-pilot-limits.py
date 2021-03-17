#!/usr/bin/env python3

import os
import time
import socket
import threading       as mt
import multiprocessing as mp

threads = list()
procs   = list()
files   = list()
sockets = list()

t_max   = 4 * 1024
p_max   = 1 * 1024
f_max   = 1 * 1024
s_max   = 1 * 1024


def _work():
    time.sleep(30)


base = '/tmp/rp_limit_%d.%%d' % os.getpid()
while True:
    try:
        f = open(base % len(files), 'w')
        files.append(f)
        if len(files) >= f_max:
            break
    except:
        break

print('files  : %5d' % len(files))
for f in files:
    os.unlink(f.name)
    f.close()

host = 'localhost'
port = 22
while True:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        sockets.append(s)
        if len(sockets) >= s_max:
            break
    except Exception:
        break

print('sockets: %5d' % len(sockets))
for s in sockets:
    s.close()

while True:
    try:
        t = mt.Thread(target=_work)
        t.start()
        threads.append(t)
        if len(threads) >= t_max:
            break
    except:
        break

print('threads: %5d' % len(threads))
for t in threads:
    t.join()


while True:
    try:
        p = mp.Process(target=_work)
        p.start()
        procs.append(p)
        if len(procs) >= p_max:
            break
    except:
        break

print('procs  : %5d' % len(procs))
for p in procs:
    p.join()


