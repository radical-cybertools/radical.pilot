#!/bin/bash

# Begin LSF Directives
#BSUB -P BIP178
#BSUB -W 0:30
#BSUB -nnodes 2
#BSUB -J jsrun_test
#BSUB -o jsrun_test.%J
#BSUB -e jsrun_test.%J

# CONFIG
path=`pwd`
d=`date +%D | sed 's/\//_/g'`
host=`/bin/hostname`
dest=$RCT_TESTS_EMAIL       # To be set in bashrc

run_cmd() {
    res_set=$1
    omp_env=$2
    cmd=$3
    exp_out=$4
    tid=$5

    # Create required folders
    rm results_$d/$tid -rf
    mkdir results_$d/$tid

    # Record resource file, omp env, and cmd in log
    echo $res_set >> results_$d/$tid/full_output.log
    echo $omp_env >> results_$d/$tid/full_output.log
    echo $cmd >> results_$d/$tid/full_output.log

    # Set omp env, create resource file and run cmd
    $omp_env
    cmd=`echo $cmd | sed "s#resource-file#$path/$tid/res_set#g"`
    $cmd >> results_$d/$tid/jsrun_output.log

    # Jsrun output to full output log file
    echo "Output:" >> results_$d/$tid/full_output.log
    cat results_$d/$tid/jsrun_output.log >> results_$d/$tid/full_output.log
}

assert() {
    act_out=$1
    exp_out=$2
    tid=$3

    failed=0

    # Remove node ids from act_out
    act_out=`cat $act_out | sed 's/Node ....../Node /g' | sed 's/OMP_threadID ./OMP_threadID /g' | sort`
    exp_out=`cat $exp_out | sort`
    if [[ ! $act_out = $exp_out ]];
    then
        echo "Expected output = $exp_out, actual output = $act_out" >> results_$d/$tid/full_output.log
        failed=1
    fi


    if [[ $failed = 1 ]];
        then
            echo "Test $tid failed. Output located in results_$d/$tid/full_output.log" >> results_$d/summary.log
        else
            echo "Test $tid passed" >> results_$d/summary.log
    fi

}


# Following tests from https://gist.github.com/vivek-bala/2bc5857e437dce2972e8faab5e886e6f

mkdir -p results_$d
touch results_$d/summary.log
for tid in test_*;
do
    res_set=`cat $tid/res_set`
    omp_env=`sed '4q;d' $tid/cmds`
    cmd=`sed '5q;d' $tid/cmds`
    exp_out="$tid/exp_out"
    echo "Running test $tid"
    run_cmd "$res_set" "$omp_env" "$cmd" "$exp_out" "$tid"
    assert "results_$d/$tid/jsrun_output.log" "$exp_out" "$tid"
done

# mail -s "Jsrun tests $d" $dest < results_$d/summary.log
mv jsrun_test.* results_$d/