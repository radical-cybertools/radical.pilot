#!/usr/bin/env python

# This is an example MPI4Py program that is used
# by different examples and tests.

import time

from mpi4py import MPI


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    host  = MPI.Get_processor_name().split('.')[0]
    comm  = MPI.COMM_WORLD

    time.sleep(1)

    print("%d/%d/%s"  % (comm.rank+1, comm.size, host))

    comm.Barrier()   # wait for everybody to synchronize here

# ------------------------------------------------------------------------------

