#!/bin/sh

export RADICAL_PILOT_DBURL=mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/
export RADICAL_PILOT_VERBOSE=DEBUG
export RADICAL_PILOT_BENCHMARK=

if false
then 
# for size in 512 1024 2048 4096
  for size in          2048 4096
  do
  
    host=stampede
    for mult in 1/2 1 2 4
    do 
  
      jobs="$(($size * $mult))"
      runtime="$(($jobs / ($size / 32) * 10 * 3/2))"
      echo "size: $size"
      echo "jobs: $jobs"
      echo "runt: $runtime"

  
      export RP_USER=tg803521
      export RP_CORES=$size
      export RP_UNITS=$jobs
      export RP_CU_CORES=32
      export RP_HOST=stampede.tacc.utexas.edu
      export RP_QUEUE=normal
      export RP_PROJECT=TG-MCB090174
      export RP_RUNTIME=$runtime
      export RP_NAME="$host.$size.$jobs"
  
      time python ./benchmark_driver.py 2>&1 | tee $RP_NAME.log
  
    done
  
  done
fi


if true
then
# for size in 512 1024 2048 4096
  for size in               4096
  do
  
    host=archer
  
  # for mult in 1/2 1 2 4
    for mult in 1/2 1 2 4
    do 
  
      jobs="$(($size * $mult))"
      runtime="$(($jobs / ($size / 32) * 10 * 3/2))"
      echo "size: $size"
      echo "jobs: $jobs"
      echo "runt: $runtime"

      export RP_USER=merzky
      export RP_CORES=$size
      export RP_UNITS=$jobs
      export RP_CU_CORES=32
      export RP_HOST=archer.ac.uk
      export RP_QUEUE=standard
      export RP_PROJECT=e290
      export RP_RUNTIME=$runtime
      export RP_NAME="$host.$size.$jobs"
  
      time python ./benchmark_driver.py 2>&1 | tee $RP_NAME.log
  
    done
  
  done
  exit
  
fi
  
