#!/usr/bin/python

#PBS -A TG-MCB090174
#PBS -l size=12
#PBS -l walltime=00:30:00

import os,sys,time
from subprocess import *

moduleString = "source /etc/profile; module swap PrgEnv-pgi PrgEnv-gnu; module load python/2.7.1-cnl;"

os.chdir(os.environ["PBS_O_WORKDIR"])

aprunString = "aprun -n 1 -d 12 -cc none -a xt python test.py"

#aprunString = "aprun -n 12 python simple.py"

os.system(moduleString + aprunString)

sys.exit(0)
