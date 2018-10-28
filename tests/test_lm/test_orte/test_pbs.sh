#!/bin/sh

. ./nodes.pbs.sh
./start_dvm.sh

# epect
echo "agent node"
echo $agent_node
aprun -n 1 -L "$agent_node" hostname

echo "compute node"
aprun -n 1 -L "$agent_node" orterun -n 1 --hnp $(cat dvm.uri) hostname

./kill_dvm.sh


