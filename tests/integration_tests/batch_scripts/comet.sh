#!/bin/bash

#SBATCH -J rp_integration_test          # job name
#SBATCH -o rp_integration_test.%j.out   # stdout file (%j expands to jobId)
#SBATCH -e rp_integration_test.%j.err   # stderr file (%j expands to jobId)
#SBATCH -p compute
#SBATCH -N 1                # total number of nodes requested (16 cores/node)
#SBATCH -n 1                # total number of mpi tasks requested
#SBATCH -t 00:30:00         # run time (hh:mm:ss) - 1.5 hours
#SBATCH -A                  # allocation name to charge job against

TEST_SLURM='radical.pilot/tests/test_resources/test_rm/test_slurm.py'
TEST_ISSUE='radical.pilot/tests/utils/integration_test_issue.py'

cdw
cd integration_tests
rm -rf radical.pilot testing
git clone --branch devel https://github.com/radical-cybertools/radical.pilot.git

module load anaconda/3.7.0

conda create -p testing python=3.7 pytest PyGithub -y

source activate testing
pip install ./radical.pilot --upgrade
pytest -vvv $TEST_SLURM > output.log 2>&1

if test "$?" = 1
then
    python $TEST_ISSUE 'SDSC Comet' output.log
    sbatch --begin='now+4weeks' comet.sh
else
    sbatch --begin='now+1week' comet.sh
fi

rm -f output.log

