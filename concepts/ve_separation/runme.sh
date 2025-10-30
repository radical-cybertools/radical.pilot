#!/bin/sh

CONDA_ROOT="$HOME/.miniconda3/"

progress(){
    printf "%-20s: ." "$*"
    i=0
    while read X
    do
        test $i -gt 64 || printf '.'
        i=$((i+1))
    done
    printf "\n"
}


# create a ve for the executor
echo
virtualenv -p python3 ve | progress 'create ve'

# create a ve for a test workload
virtualenv -p python3 ve_test | progress 'create ve_test'

# prepare a module libdir
os_path=$(python -c 'import os; print(os.__file__)')
cp $os_path modules/my_python_dir/lib/ | progress 'create module'


# create a conda env for another test workload
# run in subshell to maintain the shell env
(
    . $CONDA_ROOT/etc/profile.d/conda.sh
    conda create -y -n conda_test python=3.7 | progress 'create conda env'
)

# the forth workload will use system python - no preparation needed.
# Now run the bootstrapper which will load the executor ve and start the
# executor.  The executor will  run all three workloads.  Aim is to have all
# workload use their individual python version (ve_test, conda_test, system)
echo
./bootstrapper.sh
echo

# remove traces of this test
(
    . $CONDA_ROOT/etc/profile.d/conda.sh
    conda env remove -n conda_test 2>&1
)                                            | progress "remove conda env"
cp /dev/null modules/my_python_dir/lib/os.py | progress "remove module"
rm -rvf ve_test                              | progress "remove ve_test"
rm -rvf ve                                   | progress "remove ve"
echo

