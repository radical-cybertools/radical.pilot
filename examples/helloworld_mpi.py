#!/usr/bin/env python

# This is an example MPI4Py program that is used
# by different examples and tests.

import sys
import time
import traceback

from mpi4py import MPI

try :
    name  = MPI.Get_processor_name().split(".")[0]
    comm  = MPI.COMM_WORLD

    print "%d/%d/%s"  % (comm.rank+1, comm.size, name)

    comm.Barrier()   # wait for everybody to synchronize here

except Exception as e :
    traceback.print_exc ()
    sys.exit (1)

