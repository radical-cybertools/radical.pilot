#!/bin/bash

export RADICAL_PILOT_DBURL=mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/
export RADICAL_PILOT_VERBOSE=DEBUG
export RADICAL_PILOT_BENCHMARK=

if true
then 
  for size in 1
  do
  
    host=stampede
    for mult in 10
    do 
  
      jobs="$(($size * $mult))"
      echo "size: $size"
      echo "jobs: $jobs"
  
      export RP_USER=merzky
      export RP_CORES=$size
      export RP_UNITS=$jobs
      export RP_CU_CORES=1
      export RP_HOST=localhost
      export RP_QUEUE=
      export RP_PROJECT=
      export RP_RUNTIME=10
      export RP_NAME="$host.$size.$jobs"
  
      time python ./benchmark_driver.py 2>&1 | tee $RP_NAME.log
  
    done
  
  done
fi


if false
then 
  for size in 512 1024 2048 4096
  do
  
    host=stampede
    for mult in 1/2 1 2 4
    do 
  
      jobs="$(($size * $mult))"
      echo "size: $size"
      echo "jobs: $jobs"
  
      export RP_USER=tg803521
      export RP_CORES=$size
      export RP_UNITS=$jobs
      export RP_CU_CORES=32
      export RP_HOST=stampede.tacc.utexas.edu
      export RP_QUEUE=normal
      export RP_PROJECT=TG-MCB090174
      export RP_RUNTIME="$(($jobs * 5 / 60))"
      export RP_NAME="$host.$size.$jobs"
  
      time python ./benchmark_driver.py 2>&1 | tee $RP_NAME.log
  
    done
  
  done
fi


if false
then
  for size in 512 1024 2048 4096
  do
  
    host=archer
    if ! test $size = 1024
    then
      export RADICAL_PILOT_BENCHMARK=
    else
      unset  RADICAL_PILOT_BENCHMARK
    fi
  
    for mult in 1/2 1 2 4
    do 
  
      jobs="$(($size * $mult))"
      echo "size: $size"
      echo "jobs: $jobs"

      # only set RADICAL_PILOT_BENCHMARK if slothist size estimation is smaller than 4MB
      max_slothist_size="$((1024 * 1024 * 4))"
      est_slothist_size="$(($size  *  $jobs))"
  
      if test $est_slothist_size -gt $max_slothist_size
      then
        unset  RADICAL_PILOT_BENCHMARK
      else
        export RADICAL_PILOT_BENCHMARK=
      fi
  
      export RP_USER=merzky
      export RP_CORES=$size
      export RP_CU_CORES=32
      export RP_UNITS=$jobs
      export RP_HOST=archer.ac.uk
      export RP_QUEUE=standard
      export RP_PROJECT=e290
      export RP_RUNTIME="$(($jobs * 5 / 60))"
      export RP_NAME="$host.$size.$jobs"
  
      time python ./benchmark_driver.py 2>&1 | tee $RP_NAME.log
  
    done
  
  done
  exit
  
fi
  
