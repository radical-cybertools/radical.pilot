#!/bin/sh

dvm_uri=''
dvm_pid=''
cu_pids=''
cu_num=1000
nodes=''

# ------------------------------------------------------------------------------
#
tstamp(){
    date "+%s"
}


# ------------------------------------------------------------------------------
#
dvm_start(){
    
    echo -n 'dmv start '
    rm -f dvm.uri dvm.log
    orte-server --debug --no-daemonize --report-uri dvm.uri 2>&1 > dvm.log 2>&1 &
    dvm_pid=$!
    echo "$dvm_pid" > dvm.pid
    
    t0=$(tstamp)
    while true
    do
        test -f dvm.uri && dvm_uri=$(cat dvm.uri)
        test -z "$dvm_uri" || break
        sleep 1
        now=$(tstamp)
        test $now -gt $((t0+3)) && echo 'timeout' && break
    done
    
    test -z "$dvm_uri" && echo 'dvm startup problem' && exit
    echo $dvm_pid $dvm_uri
}


# ------------------------------------------------------------------------------
#

dvm_start

# ------------------------------------------------------------------------------

