#!/usr/bin/python

import os,sys,socket,time
from subprocess import *
import subprocess
from socket import gethostname

# these are single core executables, which are simultaneously executed on each node
commands = [
    '/bin/date',
    '/bin/date',
    '/bin/date',
    '/bin/date',
    '/bin/date',
    '/bin/date',
    '/bin/date',
    '/bin/date',
]

processes = []
outputs = []
errors = []

# simple output redirection
index = 1
host = gethostname()
base_out = 'STDOUT-' + host + '-'
base_err = 'STDERR-' + host + '-'

for cmd in commands:
    outname = base_out + str(index)
    errname = base_err + str(index)
    out = open(outname,'w')
    err = open(errname,'w')
    proc = Popen(cmd, subprocess.PIPE, shell=True, stdout=out, stderr=err)
    processes.append(proc)
    outputs.append(out)
    errors.append(err)
    index += 1

for p in processes: p.wait()
for out in outputs: out.close()
for err in errors: err.close()
                                      

