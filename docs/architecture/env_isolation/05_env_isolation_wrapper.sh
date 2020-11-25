#!/bin/sh

# shell level equivalent of utils

. ./00_env_isolation_utils.sh

# ------------------------------------------------------------------------------
# This script is started via the executor's launch method and will perform the
# following steps:
#
#   - create a barrier
#   - if rank == 0:
#     - reset the env to env.task
#     - run pre_exec commands
#     - restore env
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


sleep "0$PMIX_RANK"

. ./env.task.sh

if test "$PMIX_RANK" = 0
then
    # rank 0 - run pre_exec
    #
    # for the pre_exec, we need to unset all vars created by the launch method
    # - such as the PMIX_RANK we used just above.  Reason is that otherwise some
    # pre_exec commands such as `gmx_mpi` detect that they run under MPI and
    # collide with the MPI communicator.  We do that in a sub_shell, so that the
    # env.rank is again active.  That implies that any env changes during
    # `pre_exec_cmd` will be discarded though!
    #
    # TODO: log those changes
    # TODO: move tmp env files to /tmp

    (
        env_dump env.rank.env
        env_prep env.task.env env.rank.env env.no_rank.sh
        . ./env.no_rank.sh

        test -z "$PMIX_RANK" || echo "oops PMIX_RANK: $PMIX_RANK"

    )

    export RP_TEST=RANK0
    export RP_TEST_RANK0=True
fi

exec ./06_env_isolation_task.sh


# ------------------------------------------------------------------------------

