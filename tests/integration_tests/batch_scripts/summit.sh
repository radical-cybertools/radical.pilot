#!/bin/bash
# Begin LSF Directives
#BSUB -P GEO111
#BSUB -W 0:30
#BSUB -nnodes 1
#BSUB -alloc_flags gpumps
#BSUB -J rp_integration_test
#BSUB -o rp_integration_test.%J
#BSUB -e rp_integration_test.%J

TEST="radical.pilot/tests/integration_tests/test_rm/test_lsf.py
      radical.pilot/tests/integration_tests/test_lm/test_jsrun.py"

cd $MEMBERWORK/geo111/integration_tests/
rm -rf radical.pilot testing *.log
git clone --branch devel https://github.com/radical-cybertools/radical.pilot.git
git clone https://code.ornl.gov/t4p/Hello_jsrun.git

cd Hello_jsrun
module load cuda
make
export PATH=$PWD:$PATH
cd ../

module reset
module unload xl
module unload xalt
module unload spectrum-mpi
module unload py-pip
module unload py-virtualenv
module unload py-setuptools
module load gcc/8.1.1
module load zeromq/4.2.5
module load python/3.7.0-anaconda3-5.3.0

eval "$(conda shell.posix hook)"

conda create -p testing python=3.7 pytest PyGithub -y -c conda-forge

source activate $PWD/testing
tmpLOC=`which python`
tmpLOC=(${tmpLOC///bin/ })
tmpLOC=`find $tmpLOC/lib -name "site-packages"`/
PYTHONPATH=$tmpLOC:$PYTHONPATH


pip install ./radical.pilot --upgrade
pytest -vvv $TEST > output.log 2>&1

if test "$?" = 1
then
    echo 'Test failed'
    tests/bin/radical-pilot-test-issue -r 'ORNL Summit' -l output.log
    time=`date +'%Y:%m:%d:%H:%M:%S' -d 'now + 1 month'`
    bsub -b=$time summit.sh
else
    echo 'Everything went well'
    time=`date + '%Y:%m:%d:%H:%M:%S' -d 'now + 1 week'`
    bsub -b=$time summit.sh
fi
