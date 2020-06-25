#!/bin/sh

# this script represents the task launch script dynamically placed in the task
# sandbox.  It will run the agent's launch method to place the task on the
# respective target nodes (more specifically, it will launch the task wrapper
# script on the target nodes which will execute the task process).  The executor
# will create this script on the fly and will hardcode all require paths etc.
# It runs in the correct launcher environment already - no additional setup is
# needed

env | sort > t
(
    . ./00_env_isolation_utils.sh
    . ./env.task
    env_dump env.task.dump
)

exec mpirun -n 2 05_env_isolation_wrapper.sh

