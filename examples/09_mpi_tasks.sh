#!/bin/sh

# This script tests the correct startup of mixed OpenMP / MPI tasks via RP.
#
#   - for OpenMP, we expect RP to set OMP_NUM_THREADS to `cud.cpu_threads`.
#   - for MPI, we expect either `$PMI_RANK` (for MPICH), or `$PMIX_RANK` (for
#     OpenMPI), or ALPS_RANK (for aprun) to be set.
#
# We thus check these env settings, and each thread will print its repsective
# `process:thread` ID pair on stdout.  The consumer of this output
# (`examples/09_mpi_tasks.py`) will have to check if the correct set of ID pairs
# is found.

GPU_IDS=$(lspci | grep ' VGA ' | cut -d" " -f 1)
GPU_NUM=$(echo "$GPU_IDS" | wc -w)
CPU_NUM=$(cat /proc/cpuinfo | grep processor | wc -l)
for GPU_ID in $GPU_IDS
do
    GPU_INFO=$(lspci -v -s $GPU_ID)
done


# TODO: evaluate GPU pinning
export CUDA_VISIBLE_DEVICES="0"  # NVIDIA / CUDA   (comma separated list)
export GPU_DEVICE_ORDINAL="0"    # AMD    / OpenCL (comma separated list)
# https://stackoverflow.com/questions/43967405/
# https://stackoverflow.com/questions/14380927/ 

# OpenCV pinning to cpu or gpu
export OPENCV_OPENCL_DEVICE=":GPU:0"  # AMD    / OpenCV (outdated?) 
export OPENCV_OPENCL_DEVICE=":CPU:1"  # AMD    / OpenCV (outdated?) 
# https://docs.opencv.org/2.4/modules/ocl/doc/introduction.html

CPU_ID=$(cat /proc/$$/stat | cut -f 40 -d ' ')
GPU_ID="$CUDA_VISIBLE_DEVICES"
test -z "$GPU_ID" && GPU_ID="$GPU_DEVICE_ORDINAL"


OMP_NUM="$OMP_NUM_THREADS"
MPI_RANK="$PMI_RANK$PMIX_RANK$ALPS_APP_PE"
NODE=$(hostname)

OMP_NUM=1
test -z "$ALPS_APP_DEPTH"  || OMP_NUM="$ALPS_APP_DEPTH"
test -z "$OMP_NUM_THREADS" || OMP_NUM="$OMP_NUM_THREADS"

MPI_RANK=0
test -z "$ALPS_APP_PE"  || MPI_RANK="$ALPS_APP_PE"
test -z "$PMIX_RANK"    || MPI_RANK="$PMIX_RANK"
test -z "$PMI_RANK"     || MPI_RANK="$PMI_RANK"

seq()  (i=0; while test $i -lt $1; do echo $i; i=$((i+1)); done)
fail() (echo "$*"; exit 1)

test -z "$OMP_NUM"  && fail 'OMP_NUM_THREADS / ALPS_APP_DEPTH not set'
test -z "$MPI_RANK" && fail 'PMI_RANK / PMIX_RANK / ALPS_APP_PE not set'

for idx in $(seq $OMP_NUM); do (echo "$NODE $MPI_RANK:$idx/$OMP_NUM @ $CPU_ID/$CPU_NUM : $GPU_ID/$GPU_NUM")& done
for idx in $(seq $OMP_NUM); do wait; done

# sleep 1
# if test "$MPI_RANK" = 0
# then
#     echo
#     echo "OMP_NUM_THREADS: $OMP_NUM_THREADS"
#     echo "ALPS_APP_DEPTH : $ALPS_APP_DEPTH"
#     echo "PMI_RANK       : $PMI_RANK"
#     echo "PMIX_RANK      : $PMIX_RANK"
#     echo "ALPS_APP_PE    : $ALPS_APP_PE"
#     echo "NODE           : $(hostname)"
# fi

