#!/bin/bash
# Begin LSF Directives
#BSUB -P GEO111
#BSUB -W 0:30
#BSUB -nnodes 1
#BSUB -alloc_flags gpumps
#BSUB -J rp_integration_test
#BSUB -o rp_integration_test.%J
#BSUB -e rp_integration_test.%J

# ------------------------------------------------------------------------------
# Test files
TEST="radical.pilot/tests/integration_tests/test_rm/test_lsf.py
      radical.pilot/tests/integration_tests/test_lm/test_jsrun.py"

# ------------------------------------------------------------------------------
# Git token setup accordingly

GIT_TOKEN=

# ------------------------------------------------------------------------------
# Test folder, the same as the sbatch script submit folder
cd $MEMBERWORK/geo111/integration_tests/
rm -rf radical.pilot testing *.log
git clone --branch devel https://github.com/radical-cybertools/radical.pilot.git
git clone https://code.ornl.gov/t4p/Hello_jsrun.git

# ------------------------------------------------------------------------------
# Python distribution specific. Change if needed.

cd Hello_jsrun
module load cuda
make
export PATH=$PWD:$PATH
cd ../

module reset
module unload xl
module unload xalt
module unload spectrum-mpi
module load gcc/9.1.0
module load libzmq/4.3.3
module load python/3.7-anaconda3

eval "$(conda shell.posix hook)"

conda create -p testing python=3.7 pytest PyGithub -y -c conda-forge

source activate $PWD/testing
tmpLOC=`which python`
tmpLOC=(${tmpLOC///bin/ })
tmpLOC=`find $tmpLOC/lib -name "site-packages"`/
PYTHONPATH=$tmpLOC:$PYTHONPATH


# ------------------------------------------------------------------------------
# Test execution
pip install ./radical.pilot --upgrade
pytest -vvv $TEST > output.log 2>&1

# ------------------------------------------------------------------------------
# Test Reporting
if test "$?" = 1
then
    echo 'Test failed'
    tests/bin/radical-pilot-test-issue -r 'ORNL Summit' -l output.log
    curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token $GIT_TOKEN" \
    --request POST \
    --data '{"event_type": "test_summit", "client_payload": { "text": "failure"}}' \
    https://api.github.com/repos/radical-cybertools/radical.pilot/dispatches
    time=`date +'%Y:%m:%d:%H:%M' -d 'now + 1 month'`
    bsub -b $time summit.sh

else
    echo 'Everything went well'
    time=`date +'%Y:%m:%d:%H:%M' -d 'now + 1 week'`
    bsub -b $time summit.sh
    curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token $GIT_TOKEN" \
    --request POST \
    --data '{"event_type": "test_summit", "client_payload": { "text": "success"}}' \
    https://api.github.com/repos/radical-cybertools/radical.pilot/dispatches
fi
