#!/usr/bin/python

import os,sys,socket,time
from subprocess import *
import subprocess

commands = [
    '/lustre/scratch/antontre/python-scr/example-mpi',
    '/lustre/scratch/antontre/python-scr/example-mpi',
    '/lustre/scratch/antontre/python-scr/example-mpi',
    '/lustre/scratch/antontre/python-scr/example-mpi',
    '/lustre/scratch/antontre/python-scr/example-mpi',
    '/lustre/scratch/antontre/python-scr/example-mpi',
    '/lustre/scratch/antontre/python-scr/example-mpi',
    '/lustre/scratch/antontre/python-scr/example-mpi',
    '/lustre/scratch/antontre/python-scr/example-mpi',
    '/lustre/scratch/antontre/python-scr/example-mpi',
    '/lustre/scratch/antontre/python-scr/example-mpi',
    '/lustre/scratch/antontre/python-scr/example-mpi',
]

# run in parallel
processes = [Popen(cmd, subprocess.PIPE, shell=True)  for cmd in commands]

for p in processes: p.wait()





