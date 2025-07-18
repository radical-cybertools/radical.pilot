#!/bin/sh -l
  
#SBATCH -A dmr140125
#SBATCH --partition debug   #wholenode
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=32
#SBATCH --time=00:30:00
#SBATCH --job-name ddmd_cpu
#SBATCH --mail-user=mariya.goliyad@rutgers.edu  
#SBATCH --mail-type=ALL    # When to send emails (BEGIN, END, FAIL, ALL)

module load anaconda
source activate base
conda activate /anvil/scratch/$USER/impress/ve.impress


python /anvil/scratch/x-mgoliyad1/impress_framework/IMPRESS/src/old_ddmd/ddmd_run.py
