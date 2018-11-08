#!/bin/sh

export p=$RP_PROCESSES
export t=$RP_THREADS

val=$(( (p-3)*(p-3) + (t-5)*(t-5) ))
stat="$RP_UNIT_ID  $p  $t  $val"

# sleep 5

echo $stat >> ../app_stats.log
echo $val  >   ./app_stats.dat

echo "$p processes / $t threads"

