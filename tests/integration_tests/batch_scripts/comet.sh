#!/bin/bash

#SBATCH -J rp_integration_test          # job name
#SBATCH -o rp_integration_test.%j.out   # stdout file (%j expands to jobId)
#SBATCH -e rp_integration_test.%j.err   # stderr file (%j expands to jobId)
#SBATCH -p compute                      # partition to use
#SBATCH -N 1                            # total number of nodes (16 cores/node)
#SBATCH -n 24                           # total number of mpi tasks requested
#SBATCH -t 01:00:00                     # run time (hh:mm:ss) - 0.5 hours
#SBATCH -A                              # allocation to charge job against

# ------------------------------------------------------------------------------
# Test files
TEST="radical.pilot/tests/integration_tests/test_rm/test_slurm.py
      radical.pilot/tests/integration_tests/test_lm/test_ssh.py"

# ------------------------------------------------------------------------------
# Test folder, the same as the sbatch script submit folder
cd $SLURM_SUBMIT_DIR
rm -rf radical.pilot testing *.log
git clone --branch devel https://github.com/radical-cybertools/radical.pilot.git

# ------------------------------------------------------------------------------
# Python distribution specific. Change if needed.
module load anaconda/3.7.0
conda create -p testing python=3.7 pytest PyGithub -y
source activate $PWD/testing
tmpLOC=`which python`
tmpLOC=(${tmpLOC///bin/ })
PYTHONPATH=`find $tmpLOC/lib -name "site-packages"`/

# ------------------------------------------------------------------------------
# Test execution
pip install ./radical.pilot --upgrade
pytest -vvv $TEST > output.log 2>&1
exit_code = "$?"

# ------------------------------------------------------------------------------
# Python distribution specific. Change if needed.
conda deactivate
module unload anaconda/3.7.0

# ------------------------------------------------------------------------------
# Test Reporting
if test "$exit_code" = 1
then
    echo 'A test failed'
    radical.pilot/tests/bin/radical-pilot-test-issue -r 'SDSC Comet' -l output.log
    curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token $GIT_TOKEN" \
    --request POST \
    --data '{"event_type": "test_comet", "client_payload": { "text": "failure"}}' \
    https://api.github.com/repos/radical-cybertools/radical.pilot/dispatches
    sbatch --begin='now+4weeks' comet.sh
else
    echo 'Everything went fine'
    curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token $GIT_TOKEN" \
    --request POST \
    --data '{"event_type": "test_comet", "client_payload": { "text": "success"}}' \
    https://api.github.com/repos/radical-cybertools/radical.pilot/dispatches
    sbatch --begin='now+1week' comet.sh
fi
