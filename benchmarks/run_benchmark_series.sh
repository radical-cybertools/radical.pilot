#!/bin/sh

export RADICAL_PILOT_BENCHMARK=


# export RP_USER=merzky
# export RP_CORES=1
# export RP_CU_CORES=1
# export RP_UNITS=2
# export RP_HOST=localhost
# export RP_QUEUE=
# export RP_PROJECT=
# 
# python examples/benchmark_driver.py

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

    export RP_USER=merzky
    export RP_CORES=$size
    export RP_CU_CORES=1
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


for size in 512 1024 2048 4096
do
  echo $size

  if ! test $size = 4096
  then
    export RADICAL_PILOT_BENCHMARK=
  else
    unset  RADICAL_PILOT_BENCHMARK
  fi

  export RP_USER=tg803521
  export RP_CORES=size
  export RP_CU_CORES=1
  export RP_UNITS=size
  export RP_HOST=stampede.tacc.utexas.edu
  export RP_QUEUE=normal
  export RP_PROJECT=TG-MCB090174
  
  time python examples/benchmark_driver.py 2>&1 | tee stamede.$size.log

done

