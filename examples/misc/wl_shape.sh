#!/bin/sh

p=$RP_PROCESSES
t=$RP_THREADS

echo -n "$RP_UNIT_ID  $p  $t  $(( (p-3)*(p-3) + (t-5)*(t-5) ))" >> ../app_stats.log
echo "$p processes / $t threads"

