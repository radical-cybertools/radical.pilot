#!/bin/sh

progress(){
    printf "%-20s: " "$*"
    i=0
    while read X
    do
        test $i -gt 64 || printf '.'
        i=$((i+1))
    done
    printf "\n"
}

# create a ve for the executor
virtualenv -p python3 ve | progress 'create ve'

# create a ve for a test workload
virtualenv -p python3 ve_test | progress 'create ve_test'

# create a conda env for another test workload
# run in subshell to maintain the shell env
(
    . /home/merzky/.miniconda3/etc/profile.d/conda.sh
    conda create -y -n conda_test | progress 'create conda env'
)

# the forth workload will use system python - no preparation needed.
# Now run the bootstrapper which will load the executor ve and start the
# executor.  The executor will  run all three workloads.  Aim is to have all
# workload use their individual python version (ve_test, conda_test, system)
./bootstrapper.sh

# remove traces of this test
rm -rvf ve ve_test ~/.miniconda/envs/conda_test | progress "cleanup"

