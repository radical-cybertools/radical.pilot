#!/usr/bin/env python

import os
import time

from   mpi4py import MPI

comm = MPI.COMM_WORLD

print "Hello! I'm rank %d from %d running on %s. Taverns: %s, %s, %s." \
      % (MPI.COMM_WORLD.Get_rank(),
         MPI.COMM_WORLD.Get_size(),
         MPI.Get_processor_name(),
         os.environ.get('foo'),
         os.environ.get('sports'),
         os.environ.get('banana')
)

time.sleep(10)

comm.Barrier()   # wait for everybody to synchronize _here_

