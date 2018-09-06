#!/bin/sh
orterun --ompi-server "$(cat dvm.uri)" -n 1 hostname
