#!/bin/sh

export SBOX="$(pwd)/tmp"

ps H -ef | grep -v -e grep -e gvim | grep -e "radical\.pilot" -e "rp\." | cut -c 9-15 | xargs -rt kill -9
killall python3

mkdir -p $SBOX
rm    -f $SBOX/* *.log *.prof *.out *.err

radical-pilot-bridge    state_pubsub.json
radical-pilot-bridge    control_pubsub.json
radical-pilot-bridge    funcs_req_queue.json
radical-pilot-bridge    funcs_res_queue.json

sleep 5

export REQ_ADDR_GET=$(cat $SBOX/funcs_req_queue.cfg | grep get | cut -f 4 -d \")
export RES_ADDR_PUT=$(cat $SBOX/funcs_res_queue.cfg | grep put | cut -f 4 -d \")

# radical-pilot-component worker.0000.json
./wf0_worker.py worker.0000.json &

sleep 1

./drive_worker.py


