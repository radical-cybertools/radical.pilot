#!/bin/sh  

# This must be /bin/sh (other shells do not work)

# Run 12 copies of serial_code in the background
./example-mpi &
./example-mpi &
./example-mpi &
./example-mpi &
./example-mpi &
./example-mpi &
./example-mpi &
./example-mpi &
./example-mpi &
./example-mpi &
./example-mpi &
./example-mpi &

# Wait until all copies of serial_code have finished
wait

