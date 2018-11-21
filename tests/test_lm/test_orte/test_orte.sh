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
dvm_stop(){
    echo "dvm stop $dvm_pid"
    kill $dvm_pid
}


# ------------------------------------------------------------------------------
#
cus_start(){

    echo -n 'cus start '

    n=0
    while test $n -lt $cu_num
    do
        orterun --ompi-server "$(cat dvm.uri)" -n 1 /bin/sleep 1 2>&1 >> run.log 2>&1 &
        cu_pids="$cu_pids $!"
        n=$((n+1))
        echo -n .
    done
    echo

    # orterun --ompi-server "$(cat dvm.uri)" -n 1 /bin/false 2>&1 >> run.log 2>&1 &
    # pids="$cu_pids $!"
}



# ------------------------------------------------------------------------------
#
cus_wait(){

    echo -n 'cus wait  '

    for pid in $cu_pids
    do
        wait $pid
        test $? = 0 && echo -n '+' || echo -n '-' 
    done
    echo
}

# ------------------------------------------------------------------------------
#
work(){

    for wl in $(echo workload/wl.*)
    do
        echo $wl
        workload_start $wl
        workload_wait  $wl
    done
}

# ------------------------------------------------------------------------------
#
workload_start(){

    wl=$1
    echo -n $wl
    for u in $(echo $wl/unit.*)
    do
        cmd=$(  cat $u | grep -e '^command ' | cut -f 2- -d : | xargs echo)
        slots=$(cat $u | grep -e '^slots '   | cut -f 2- -d : | xargs echo)
        cores=$(cat $u | grep -e '^cores '   | cut -f 2- -d : | xargs echo)
        ret=$(  cat $u | grep -e '^retval '  | cut -f 2- -d : | xargs echo)

        cmd="orterun --ompi-server $dvm_uri -n $cores -H $slots $cmd"
        echo $cmd
        $cmd 2>&1 >> $u.log 2>&1 &
        cu_pids="$cu_pids $!"
        echo -n .
    done
    echo
}

# ------------------------------------------------------------------------------
#
workload_wait(){

    wl=$1
    echo -n $wl

    for pid in $cu_pids
    do
        wait $pid
        test $? = 0 && echo -n '+' || echo -n '-' 
    done
    echo
    cu_pids=''
}

# ------------------------------------------------------------------------------
#

dvm_start
work
dvm_stop

# ------------------------------------------------------------------------------

