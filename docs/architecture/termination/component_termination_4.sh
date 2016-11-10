#!/bin/sh

TIMEOUT=5

log="component_termination_4.log"

if test -f $log
then
    i=1
    while test -f "$log.$i"
    do
        i=$((i+1))
    done
    mv -v $log $log.$i
fi

mkdir -p errlog
export i=0
while true
do
    i=$((i+1))
    (
        touch component_termination_4.ok
        (
            sleep $TIMEOUT
            echo 'timed out'
            rm  -f component_termination_4.ok
            ps -ef | grep merzky | grep -v grep | grep -e rp.main -e python | cut -c 10-15 | xargs -rt kill
        ) &
        watchpid=$!
        python ./component_termination_4.py 2>&1 > component_termination_4.tmp 2>&1
        kill -9 $watchpid > /dev/null 2>&1

        if test -e component_termination_4.ok
        then
            printf "%5d OK   `date`\n" $i | tee -a $log
            rm -f component_termination_4.tmp
        else
            printf "%5d FAIL `date`\n" $i | tee -a $log
            grep -e '^RuntimeError' component_termination_4.tmp | tee -a $log
            mv component_termination_4.tmp errlog/component_termination_4.tmp.$i
        fi
        rm -f component_termination_4.ok
    )
done

