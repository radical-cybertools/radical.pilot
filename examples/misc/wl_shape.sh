#!/bin/sh

p=$RP_PROCESSES
t=$RP_THREADS

echo "sqrt(($p-3)^2) + sqrt(($t-5)^2)" | bc > ./app_stats.dat
echo "$p processes / $t threads"

