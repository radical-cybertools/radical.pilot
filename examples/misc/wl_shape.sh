#!/bin/sh

export p=$RP_PROCESSES
export t=$RP_THREADS

echo "$RP_UNIT_ID  $p  $t  $(( (p-3)*(p-3) + (t-5)*(t-5) ))" >> ../app_stats.log
echo "$p processes / $t threads"

