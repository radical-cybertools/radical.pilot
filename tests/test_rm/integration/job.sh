# virtualenv for rp 
# source ~/venv/rp/bin/activate
# conda activate rp
resource=$1
lrms=`echo $2| tr '[:upper:]' '[:lower:]'`
res=`pytest tests/test_rm/integration/test_rm_int.py::test_rm_$lrms --resource $resource -vvvvv -p no:warnings`
if [ $? -ne 0 ]
then
	echo $res
fi

