#!/bin/bash
#SBATCH -J rp_integration_test  # Job name
#SBATCH -p compute
#SBATCH -o rp_integration_test.%j.out   # Name of stdout output file(%j expands to jobId)
#SBATCH -e rp_integration_test.%j.err   # Name of stderr output file(%j expands to jobId)
#SBATCH -N 1                # Total number of nodes requested (16 cores/node)
#SBATCH -n 1                # Total number of mpi tasks requested
#SBATCH -t 01:00:00         # Run time (hh:mm:ss) - 1.5 hours
# The next line is required if the user has more than one project
#SBATCH -A  # Allocation name to charge job against

TEST="radical.pilot/tests/integration_tests/test_rm/test_slurm.py
      radical.pilot/tests/integration_tests/test_lm/test_ssh.py"

cd $SLURM_SUBMIT_DIR
rm -rf radical.pilot testing *.log
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
    echo 'A test failed'
    radical.pilot/tests/bin/radical-pilot-test-issue -r 'SDSC Comet' -l output.log
    curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token $GIT_TOKEN" \
    --request POST \
    --data '{"event_type": "test_result", "client_payload": { "text": "failure"}}' \
    https://api.github.com/repos/radical-cybertools/radical.pilot/comet
    sbatch --begin='now+4weeks' comet.sh
else
    echo 'Everything went fine'
    curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token $GIT_TOKEN" \
    --request POST \
    --data '{"event_type": "test_result", "client_payload": { "text": "success"}}' \
    https://api.github.com/repos/radical-cybertools/radical.pilot/comet
    sbatch --begin='now+1week' comet.sh
fi
