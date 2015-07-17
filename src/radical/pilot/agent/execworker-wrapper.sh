#!/bin/sh

SPAWNER=$1
WORK=$2
P1=$3
P2=$4

runme(){
    port=$1
    fifo="/tmp/fifo.${port}"
    rm -rf $fifo
    mkfifo $fifo
    nc -l $port < $fifo | /bin/sh ${SPAWNER} ${WORK} > $fifo
}

(runme $P1) &
(runme $P2)
