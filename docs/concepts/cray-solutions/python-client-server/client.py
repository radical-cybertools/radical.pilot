#!/usr/bin/python

import sys,os,getopt
import socket,subprocess
from subprocess import *

# initially should be 'at-server-at'
HOST = '@server@'
PORT = 8000

print 'port at compute: %s' % str(PORT)
print 'host at compute: %s' % str(HOST)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))

taskString = ' '
while (str(taskString) != 'STOP'):
    s.sendall('SEND')
    taskString = s.recv(1024)
    print 'Received: ', repr(taskString)

    if (str(taskString) != 'STOP'):
        tasks = taskString.split(' ',1)

        # run in parallel
        processes = [Popen(task, subprocess.PIPE, shell=True) for task in tasks]
        for p in processes: p.wait()
    else:
        s.close()

