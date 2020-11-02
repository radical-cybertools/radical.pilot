#!/bin/sh

export SBOX="$(pwd)/tmp"

mkdir -p $SBOX
rm    -f $SBOX/*

radical-pilot-bridge    state_pubsub.json
radical-pilot-bridge    control_pubsub.json
radical-pilot-bridge    funcs_req_queue.json
radical-pilot-bridge    funcs_res_queue.json

export REQ_ADDR_GET=$(cat $SBOX/funcs_req_queue.cfg | grep get | cut -f 4 -d \")
export RES_ADDR_PUT=$(cat $SBOX/funcs_res_queue.cfg | grep put | cut -f 4 -d \")

radical-pilot-component worker.0000.json

sleep 1

./drive_worker.py


