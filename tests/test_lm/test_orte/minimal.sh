#!/bin/bash

# ------------------------------------------------------------------------------
#
set -e
set -u

rm -f run.log
rm -f dvm.log

TOT=$((1024*128))
MAX=1024
BATCH=128

OMPI_ROOT=/home/merzky/radical/ompi/installed/2017_09_18_539f71d
export PATH=$OMPI_ROOT/bin:$PATH
export PYTHONPATH=''


# ------------------------------------------------------------------------------
#
dvm_start(){
    /usr/bin/stdbuf -oL \
              orte-server \
              --no-daemonize \
              --report-uri ./dvm.uri \
              --debug \
              > dvm.log 2>&1 &
    dvm_pid=$!

    while true
    do
        if ! test -f dvm.uri
        then
            sleep 1
            continue
        fi
    
        dvm_uri=$(grep '://' dvm.uri)
        test -z "$dvm_uri" && continue
        break
    done
    
    echo "dvm started: $dvm_uri [$dvm_pid]"
}


# ------------------------------------------------------------------------------
#
dvm_stop(){
    kill -9 $dvm_pid
    echo "dvm stopped: $dvm_uri [$dvm_pid]"
    echo
}


# ------------------------------------------------------------------------------
#
unit_start(){
    orterun --ompi-server "$dvm_uri" \
            -np 1 \
            --bind-to none \
            -host localhost \
            -x "LD_LIBRARY_PATH" -x "PATH" -x "PYTHONPATH"  \
            /bin/true >> run.log 2>&1 &
    echo -n '.'
}


# ------------------------------------------------------------------------------
#
unit_wait(){
    wait -n
    echo -n '+'
}


# ------------------------------------------------------------------------------
#
t0=$(date "+%s")
dvm_start
t1=$(date "+%s")


# ------------------------------------------------------------------------------
# warmup
n=0
while test $n -lt $MAX; do
    unit_start
    n=$((n+1))
done
echo " $n"


# ------------------------------------------------------------------------------
# wait/run batches until done
while test $n -lt $TOT; do
    b=0
    while test $b -lt $BATCH; do
        unit_wait
        b=$((b+1))
    done
    echo

    b=0
    while test $b -lt $BATCH; do
        unit_start
        b=$((b+1))
    done
    echo " $n"

    n=$((n+b))
done
echo


# ------------------------------------------------------------------------------
# wait for last batch
b=0
while test $b -lt $MAX; do
    unit_wait
    b=$((b+1))
done
echo


# ------------------------------------------------------------------------------
t2=$(date "+%s")
dvm_stop
t3=$(date "+%s")


# ------------------------------------------------------------------------------
printf "start: %4.2f" $((t1-t0))
printf "run  : %4.2f" $((t2-t1))
printf "stop : %4.2f" $((t3-t2))
printf "total: %4.2f" $((t3-t0))


# ------------------------------------------------------------------------------

