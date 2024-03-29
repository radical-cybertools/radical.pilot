#!/bin/sh
# test the correct startup of mixed OpenMP / MPI / CPU / GPU tasks

test -z "$RP_PROF" && RP_PROF=true

$RP_PROF "app_start"

# basic information
SLEEP=$1
WORLD=$2
PID=$$
NODE=$(hostname)

test -z "$SLEEP" && SLEEP=0

# get MPI rank
MPI_RANK=""
test -z "$MPI_RANK" && MPI_RANK="$ALPS_APP_PE"
test -z "$MPI_RANK" && MPI_RANK="$PMIX_RANK"
test -z "$MPI_RANK" && MPI_RANK="$PMI_RANK"
test -z "$MPI_RANK" && MPI_RANK="$SLURM_PROCID"
test -z "$MPI_RANK" && MPI_RANK="0"

# obtain number of threads
THREAD_NUM=""
test -z "$THREAD_NUM" && THREAD_NUM="$ALPS_APP_DEPTH"
test -z "$THREAD_NUM" && THREAD_NUM="$OMP_NUM_THREADS"
test -z "$THREAD_NUM" && THREAD_NUM="1"

# obtain info about CPU pinning
CPU_MASK=$(cat /proc/$PID/status \
        | grep -i 'Cpus_allowed:' \
        | cut -f 2 -d ':' \
        | xargs -n 1 echo \
        | sed -e 's/,//g' \
        | tr 'a-f' 'A-F')

CPU_BITS=$(echo "obase=2; ibase=16; $CPU_MASK" | \bc | tr -d '\\[:space:]')
CPU_BLEN=$(echo $CPU_BITS | wc -c)
CPU_NBITS=$(cat /proc/cpuinfo | grep processor | wc -l)
while test "$CPU_BLEN" -le "$CPU_NBITS"
do
    CPU_BITS="0$CPU_BITS"
    CPU_BLEN=$((CPU_BLEN+1))
done

# same information about GPU pinning
# (assume we have no more GPUs than cores)
GPU_INFO=""
test -z "$GPU_INFO" && GPU_INFO="$CUDA_VISIBLE_DEVICES"
test -z "$GPU_INFO" && GPU_INFO="$GPU_DEVICE_ORDINAL"
GPU_INFO=$(echo " $GPU_INFO " | tr ',' ' ')

USERID=$(id -u)
LSPCI_CACHE="/tmp/lspci.$USERID"
if ! test -f "$LSPCI_CACHE"
then
    LSPCI=$(which lspci 2> /dev/null)
    test -z "$LSPCI" && LSPCI='/sbin/lspci'
    test -f "$LSPCI" || LSPCI='/usr/sbin/lspci'
    test -f "$LSPCI" || LSPCI='true'
    $LSPCI | grep -e ' VGA ' -e ' GV100GL ' -e 'ATI' | wc -l > "$LSPCI_CACHE"
fi

GPU_NBITS=$(cat "$LSPCI_CACHE")
GPU_BITS=''
n=0
while test "$n" -lt "$GPU_NBITS"
do
    if echo "$GPU_INFO" | grep -e " $n " >/dev/null
    then
        GPU_BITS="1$GPU_BITS"
    else
        GPU_BITS="0$GPU_BITS"
    fi
    n=$((n+1))
done

# # redireect js_task_info to stderr (if available, i.e. on summit)
# JS_TASK_INFO=/opt/ibm/spectrum_mpi/jsm_pmix/bin/js_task_info
# test -f $JS_TASK_INFO && $JS_TASK_INFO "$TGT.info" 1>&2

PREFIX="$MPI_RANK"
test -z "$PREFIX" && PREFIX='0'

if test -z "$WORLD"
then
    printf "$PREFIX : PID     : $PID\n"
    printf "$PREFIX : NODE    : $NODE\n"
    printf "$PREFIX : CPUS    : $CPU_BITS\n"
    printf "$PREFIX : GPUS    : $GPU_BITS\n"
    printf "$PREFIX : RANK    : $MPI_RANK\n"
    printf "$PREFIX : THREADS : $THREAD_NUM\n"
    printf "$PREFIX : SLEEP   : $SLEEP\n"

else
    echo "hello $WORLD"
fi

# check if `stress-ng` is installed
export PATH="$PATH:$RADICAL_RESOURCE_SBOX/install/bin"
STRESS=$(which stress-ng 2> /dev/null)

# if so requested, sleep/stress for a bit
if test -z "$STRESS"
then
    sleep $SLEEP
else
    $STRESS -c 1 -t ${SLEEP}s 2> /dev/null
fi


$RP_PROF "app_stop"

