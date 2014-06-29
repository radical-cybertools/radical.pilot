#!/bin/env python
from mpi4py import MPI;
import sys
import time
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
name = MPI.Get_processor_name()
sys.stdout.write("Hello, World! I am process %d of %d on %s.\n"  % (rank, size, name))
SLEEP = 10
sys.stdout.write("Sleeping for %d seconds ..." % SLEEP)
time.sleep(SLEEP)
