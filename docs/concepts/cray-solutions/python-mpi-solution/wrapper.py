#!/usr/bin/python

import os,sys,socket,time
from subprocess import *
import subprocess
from mpi4py import MPI
from subprocess import call


exctbl = "example-mpi"

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

myDir = "/lustre/scratch/antontre/python-scr/"

cmd = "cd "+myDir+" ; "+exctbl+" "
sts = call(cmd,shell=True)

comm.Barrier()
