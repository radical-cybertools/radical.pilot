#!/usr/bin/env python

from mpi4py import MPI
import numpy

comm = MPI.Comm.Get_parent()
size = comm.Get_size()
rank = comm.Get_rank()

N = numpy.array(0, dtype='i')

comm.Bcast([N, MPI.INT], root=0)

h = 1.0 / N; s = 0.0

for i in range(rank, N, size):
    x = h * (i + 0.5)
    s += 4.0 / (1.0 + x**2)

PI = numpy.array(s * h, dtype='d')

comm.Reduce([PI, MPI.DOUBLE], None,op=MPI.SUM, root=0)

comm.Disconnect()

