#!/bin/bash

PORT=59465
$HOME/redis/redis-stable/src/redis-server --port $PORT --protected-mode no --loglevel debug &> redis.out &


#REDIS=$!

export RADICAL_LOG_LVL="DEBUG"
export RADICAL_PROFILE="TRUE"
export RADICAL_PILOT_DBURL=mongodb://usrname:password@ip:port/db_name

#echo "Redis started on $HOSTNAME:$PORT"

python colmena_rp.py\
       --redis-host $HOSTNAME \
       --redis-port $PORT \
       #--task-input-size 0 \
       #--task-output-size 0 \
       #--task-interval 0.1 \
       #--task-count 50 \
       #--output-dir runs/full_test_unique_30s_50x50_v3 \

# Kill the redis server
#kill $REDIS