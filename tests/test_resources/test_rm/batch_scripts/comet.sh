#!/bin/bash
#SBATCH -J rp_integration_test  # Job name
#SBATCH -p compute
#SBATCH -N 1                # Total number of nodes requested (16 cores/node)
#SBATCH -n 1                # Total number of mpi tasks requested
#SBATCH -t 00:30:00         # Run time (hh:mm:ss) - 1.5 hours
# The next line is required if the user has more than one project
#SBATCH -A # Allocation name to charge job against

cdw
cd integration_tests
rm -rf *
git clone --branch  feature/res_int_test https://github.com/radical-cybertools/radical.pilot.git

module load anaconda/3.7.0

conda create -p testing python=3.7 pytest PyGithub -y

source activate testing
cd radical.pilot
pip install . --upgrade
pytest -vvv tests/test_resources/test_rm/test_slurm.py > output.log 2>&1

if test "$?" = 1
then
    python radical.pilot/tests/utils/issue.py output.log
fi