# virtualenv for rp 
# source ~/venv/rp/bin/activate
# conda activate rp
resource=$1
launch_metod=`echo $2| tr '[:upper:]' '[:lower:]'`
res=`pytest tests/test_lm/integration/test_lm_int.py::test_lm_$launch_method --resource $resource -vvvvv -p no:warnings`
if [ $? -ne 0 ]
then
	echo $res
fi

