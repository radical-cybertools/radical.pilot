#!/bin/sh

SPAWNER=$1
WORKDIR_BASE=$2
PORT_BASE=$3
WORKER_COUNT=$4

usage() {
    echo "Usage: $0 <spawner.sh> <workdir> <port base> <count>"
    exit 1
}

if [ $# -ne 4 ]; then
    usage
fi

#
# Start a Spawner and connect it to the outside world through netcat
#
start_spawner(){
    workdir=$1
    port=$2

    fifo="$WORKDIR_BASE/fifo.$port"
    rm -rf $fifo
    mkfifo $fifo
    nc -l $port < $fifo | /bin/sh $SPAWNER $workdir > $fifo
}

#
# Create two spawner instances per ExecWorker (Spawner + Monitor)
#
for worker_num in $(seq 0 $(expr $WORKER_COUNT - 1))
do
    workdir="$WORKDIR_BASE/ExecWorker-$worker_num"
    mkdir -p $workdir

    start_spawner $workdir $(expr $PORT_BASE + $worker_num \* 2) &
    start_spawner $workdir $(expr $PORT_BASE + $worker_num \* 2 + 1) &
done

wait
