#!/bin/bash
# Begin LSF Directives
#BSUB -P BIP179
#BSUB -W 0:05
#BSUB -nnodes 1
#BSUB -J rp_pytest
#BSUB -o rp_pytest.%J
#BSUB -e rp_pytest.%J
# virtualenv for rp 
# source ~/venv/rp/bin/activate
. /sw/summit/python/2.7/anaconda2/5.3.0/etc/profile.d/conda.sh
conda activate rp
resource='xsede.summit'
launch_method='jsrun'
#`echo $2| tr '[:upper:]' '[:lower:]'`
res=`jsrun -n 1 -r 1 -a 1 pytest tests/test_lm/integration/test_lm_int.py::test_lm_$launch_method --resource $resource -vvvvv -p n
o:warnings`
if [ $? -ne 0 ]
then
		echo $res
	fi
