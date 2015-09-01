#!/usr/bin/env python

# This is an example MPI4Py program that is used
# by different examples and tests.

import sys
import time
import traceback

from mpi4py import MPI

try :
    SLEEP = 10
    name  = MPI.Get_processor_name()
    comm  = MPI.COMM_WORLD

    print "mpi rank %d/%d"  % (comm.rank, comm.size)

    time.sleep(SLEEP)

    comm.Barrier()   # wait for everybody to synchronize here

except Exception as e :
    traceback.print_exc ()
    print "error : %s" % e
    sys.exit (1)

finally :
    sys.exit (0)

