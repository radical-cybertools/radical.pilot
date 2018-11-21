#!/bin/sh
orterun --hnp "$(cat dvm.uri)" -n 1 hostname
