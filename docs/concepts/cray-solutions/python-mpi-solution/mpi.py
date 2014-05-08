#!/usr/bin/python

#PBS -A TG-MCB090174
#PBS -l size=12
#PBS -l walltime=00:30:00

import os,sys,time

moduleString = "source /etc/profile; module swap PrgEnv-pgi PrgEnv-gnu; module load python/2.7.1-cnl;"

os.chdir(os.environ["PBS_O_WORKDIR"])

aprunString = "aprun -n 12 python wrapper.py"

os.system(moduleString + aprunString)

sys.exit(0)
