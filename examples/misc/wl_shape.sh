#!/bin/sh

p=$RP_PROCESSES
t=$RP_THREADS

<<<<<<< HEAD
echo -n "$RP_UNIT_ID  $p  $t  $(( (p-3)*(p-3) + (t-5)*(t-5) ))" >> ../app_stats.log
=======
val=$(( (p-3)*(p-3) + (t-5)*(t-5) ))
stat="$RP_UNIT_ID  $p  $t  $val"

echo $stat >> ../app_stats.log
echo $val  >   ./app_stats.dat

>>>>>>> 0b82bd3f... better eval of stats
echo "$p processes / $t threads"

