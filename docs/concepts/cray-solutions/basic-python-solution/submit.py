#!/usr/bin/python

# submit this file as:
# $qsub submit.py 

#PBS -A e290
# base name of the output file
#PBS -N mpi-test-1
# we select two nodes
#PBS -l select=2
#PBS -l walltime=00:05:00

import sys,os,time

# chose execuiton mode: single core jobs mode='single'; mpi jobs mode='mpi' 
mode = 'single'

# chose the number of mpi jobs to execute
jobs = 5

# aprun block for ARCHER
#############################################################################################
# same for both modes
moduleString = "source /etc/profile \n export PBS_O_WORKDIR=$(readlink -f $PBS_O_WORKDIR) \n"

os.chdir(os.environ["PBS_O_WORKDIR"])
os.system(moduleString)

if (mode == 'single'):
    # aprun string for single core jobs on multiple nodes; wrapper.py executes jobs simultaneously
    # same pattern for each of the two allocated nodes, e. g. fixed number of single core jobs for each node
    aprunString = "aprun -n 2 -d 24 -cc none -a xt python wrapper.py"
    #os.chdir(os.environ["PBS_O_WORKDIR"])
    os.system(aprunString)

if (mode == 'mpi'):
    # aprun string for each of the mpi executables
    aprunString = "aprun -n 1 /bin/hostname "
    waitString = "wait"
    # run four bin/hostname on two nodes submitting to each node separately two bin/hostname's will be executed simultaneously
    # this can be extended to as many submissions as required, e.g. we must know in advance how many mpi jobs to execute

    for i in range(0,(jobs-1)):
        os.chdir(os.environ["PBS_O_WORKDIR"])
        jobString = aprunString + "> STDOUT-" + str(i) + ".dat & \n"
        os.system(jobString)

    # last submission for some reason must be executed together with wait
    jobString = aprunString + "> STDOUT-" + str(jobs) + ".dat & \n"
    os.chdir(os.environ["PBS_O_WORKDIR"])
    os.system(jobString + waitString)

sys.exit(0)

