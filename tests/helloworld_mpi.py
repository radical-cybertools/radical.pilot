#!/usr/bin/env python

# This is an example MPI4Py program that is used
# by different examples and tests.

from   mpi4py import MPI
import time

SLEEP = 10
name  = MPI.Get_processor_name()
comm  = MPI.COMM_WORLD

print "mpi rank %d/%d/%s"  % (comm.rank, comm.size, name)

time.sleep(SLEEP)

comm.Barrier()   # wait for everybody to synchronize here

