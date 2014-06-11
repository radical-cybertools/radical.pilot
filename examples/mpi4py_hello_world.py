#!/usr/bin/env python

# This is an example MPI4Py program that is used
# by the 'mpi_executables.py' examlple.

from mpi4py import MPI


comm = MPI.COMM_WORLD

print "Hello! I'm rank %d from %d running in total..." % (comm.rank, comm.size)

comm.Barrier()   # wait for everybody to synchronize _here_
