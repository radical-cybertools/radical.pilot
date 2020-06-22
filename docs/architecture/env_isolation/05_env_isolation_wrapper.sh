#!/bin/sh

# ------------------------------------------------------------------------------
# This script is started via the executor's launch method and will perform the
# following steps:
#
#   - reset the env to env.task
#   - create a barrier
#   - if rank == 0:
#     - run pre_exec commands
#     - release barrier
#   - if rank > 0:
#     - wait for barrier
#   - all ranks:
#     - run task executable
#
# The barrier could be implemented via the file system, or via small utilities
# which exchange messages via ZMQ, or rely on the application's `MPI_INIT`
# barrier.  RP must ensure a barrier exists for all LM types and task types.
#
# The switch for rank ID will have to be launch method specific - that is why
# this script has to be dynamically created by the executor. Here is an example
# for OpenMPI, where the barrier is basically provided by the application's
# `MPI_INIT` call (in fact, the MPI standard seems not to accomodate a timeout
# for `MPI_INIT`, and versions of this method should work for all MPI
# implementations):

sleep $OMPI_COMM_WORLD_RANK

echo "-- task rank $OMPI_COMM_WORLD_RANK --"
. ./env.task

if test "$OMPI_COMM_WORLD_RANK" = 0
then
    # rank 0 - run pre_exec
    echo 'run pre_exec'
    export RP_TEST=RANK0
    export RP_TEST_RANK0=True

  # sleep 3
fi

echo "run task"
exec ./06_env_isolation_task.sh


# ------------------------------------------------------------------------------

