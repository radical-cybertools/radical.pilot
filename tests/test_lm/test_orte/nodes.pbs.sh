#!/bin/sh

cp -v $PBS_NODEFILE nodes.pbs
agent_node=$(cat nodes.pbs  | sort -u | tail -n 1)
cat nodes.pbs | grep -v "$agent_node" > nodes.dvm

echo "agent   node : $agent_node"
echo "compute nodes: $(cat nodes.dvm | sort -u | xargs echo)"

