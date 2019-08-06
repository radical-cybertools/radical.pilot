#virtualenv for rp 
res=`pytest test_rm_int.py::test_rm_slurm --resource $1 -vvv`
if [ $? -ne 0 ]
then
	echo $res
fi

