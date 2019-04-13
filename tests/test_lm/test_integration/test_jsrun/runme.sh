#!/bin/bash

# current directory
cd /ccs/home/vivekb/test_jsrun

# Run the jobs file 
bsub jobs.sh

# Chain next job
at now + 1 day -f runme.sh -m
