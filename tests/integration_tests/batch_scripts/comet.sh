#!/bin/bash
#SBATCH -J rp_integration_test  # Job name
#SBATCH -o rp_integration_test.%j.out   # Name of stdout output file(%j expands to jobId)
#SBATCH -e rp_integration_test.%j.err   # Name of stderr output file(%j expands to jobId)
#SBATCH -p compute
#SBATCH -N 1                # Total number of nodes requested (16 cores/node)
#SBATCH -n 1                # Total number of mpi tasks requested
#SBATCH -t 00:30:00         # Run time (hh:mm:ss) - 1.5 hours
# The next line is required if the user has more than one project
#SBATCH -A  # Allocation name to charge job against
#SBATCH -C EGRESS

TEST="radical.pilot/tests/integration_tests/test_rm/test_slurm.py"

cdw
cd integration_tests
rm -rf radical.pilot testing
git clone --branch devel https://github.com/radical-cybertools/radical.pilot.git

module load anaconda/3.7.0

conda create -p testing python=3.7 pytest PyGithub -y

source activate $PWD/testing
tmpLOC=`which python`
tmpLOC=(${tmpLOC///bin/ })
PYTHONPATH=`find $tmpLOC/lib -name "site-packages"`/

pip install ./radical.pilot --upgrade
pytest -vvv $TEST > output.log 2>&1

if test "$?" = 1
then
    radical.pilot/tests/bin/radical-pilot-test-issue -r 'SDSC Comet' -l output.log
    sbatch --begin='now+4weeks' comet.sh 
else
    sbatch --begin='now+1week' comet.sh 
fi