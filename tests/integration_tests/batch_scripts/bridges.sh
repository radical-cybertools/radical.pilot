#!/bin/bash
<<<<<<< HEAD
#!/bin/bash

#SBATCH -J rp_integration_test          # job name
#SBATCH -o rp_integration_test.%j.out   # stdout file (%j expands to jobId)
#SBATCH -e rp_integration_test.%j.err   # stderr file (%j expands to jobId)
#SBATCH -p compute                      # partition to use
#SBATCH -N 1                            # total number of nodes (16 cores/node)
#SBATCH -n 1                            # total number of mpi tasks requested
#SBATCH -t 00:30:00                     # run time (hh:mm:ss) - 0.5 hours
#SBATCH -A                              # allocation to charge job against
#SBATCH -C EGRESS

# ------------------------------------------------------------------------------
# Test files
TEST_SLURM='radical.pilot/tests/integration_tests/test_rm/test_slurm.py'
TEST_ISSUE='radical.pilot/tests/bin/radical-pilot-test-issue'

# ------------------------------------------------------------------------------
# Test folder, the same as the sbatch script submit folder
=======
#SBATCH -J rp_integration_test  # Job name
#SBATCH -o rp_integration_test.%j.out   # Name of stdout output file(%j expands to jobId)
#SBATCH -e rp_integration_test.%j.err   # Name of stderr output file(%j expands to jobId)
#SBATCH -p RM
#SBATCH -N 1                # Total number of nodes requested (16 cores/node)
#SBATCH -n 28                # Total number of mpi tasks requested
#SBATCH -t 01:00:00         # Run time (hh:mm:ss) - 1.5 hours
# The next line is required if the user has more than one project
#SBATCH -A  # Allocation name to charge job against
#SBATCH -C EGRESS

TEST="radical.pilot/tests/integration_tests/test_rm/test_slurm.py
      radical.pilot/tests/integration_tests/test_lm/test_ssh.py"

>>>>>>> devel
cd $SLURM_SUBMIT_DIR
rm -rf radical.pilot testing *.log
git clone --branch devel https://github.com/radical-cybertools/radical.pilot.git

<<<<<<< HEAD
# ------------------------------------------------------------------------------
# Python distribution specific. Change if needed.
module load python3conda/3.8.5
conda create -p testing python=3.7 pytest PyGithub -y
source activate $PWD/testing
tmpLOC=`find $PWD/lib -name "site-packages"`/
PYTHONPATH=$tmpLOC:$PYTHONPATH

# ------------------------------------------------------------------------------
# Test execution
=======
module reset
module load gcc
module load mpi/gcc_openmpi
module load slurm
module load anaconda3

conda create -p testing python=3.7 pytest PyGithub -y -c conda-forge

source activate $PWD/testing
tmpLOC=`which python`
tmpLOC=(${tmpLOC///bin/ })
tmpLOC=`find $tmpLOC/lib -name "site-packages"`/
PYTHONPATH=$tmpLOC:$PYTHONPATH

>>>>>>> devel
pip install ./radical.pilot --upgrade
pytest -vvv $TEST > output.log 2>&1

if test "$?" = 1
then
<<<<<<< HEAD
    $TEST_ISSUE -r 'SDSC Comet' -l output.log
    sbatch --begin='now+4weeks' comet.sh 
else
    sbatch --begin='now+1week' comet.sh 
=======
    echo 'Test failed'
    tests/bin/radical-pilot-test-issue -r 'PSC Bridges' -l output.log
    sbatch --begin='now+4weeks' bridges.sh
else
    echo 'Everything went well'
    sbatch --begin='now+1week' bridges.sh
>>>>>>> devel
fi
