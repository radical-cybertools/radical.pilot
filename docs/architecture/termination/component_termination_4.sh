#!/bin/sh

export i=0
TIMEOUT=30

while true
do
    i=$((i+1))
    (
        touch component_termination_4.ok
        (
            sleep $TIMEOUT
            echo 'timed out'
            rm  -f component_termination_4.ok
            ps -ef | grep merzky | grep -v grep | grep python | cut -c 10-15 | xargs kill
        ) &
        watchpid=$!
        python ./component_termination_4.py 2>&1 > component_termination_4.tmp 2>&1
        kill -9 $watchpid > /dev/null 2>&1

        if test -e component_termination_4.ok
        then
            printf "%5d OK   `date`\n" $i | tee -a component_termination_4.log
        else
            printf "%5d FAIL `date`\n" $i | tee -a component_termination_4.log
            grep -e '^RuntimeError' component_termination_4.tmp | tee -a component_termination_4.log
        fi
        rm -f component_termination_4.tmp
        rm -f component_termination_4.ok
    )
done

