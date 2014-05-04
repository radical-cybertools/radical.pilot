#!/usr/bin/python

#PBS -A e290
#PBS -N run-script
#PBS -l mppwidth=512
#PBS -l mppnppn=32
#PBS -l walltime=00:15:00

import sys,os,time

# aprun block for HECTOR
################################################################################################################
moduleString = "source /etc/profile; module swap PrgEnv-cray PrgEnv-gnu; module load python-shared-xe6/2.7.3;"
os.chdir(os.environ["PBS_O_WORKDIR"])
aprunString = "aprun -n 16 -d 32 -cc none -a xt python client.py"
os.system(moduleString + aprunString)
################################################################################################################


sys.exit(0)

