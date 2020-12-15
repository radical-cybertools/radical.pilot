#!/bin/bash

# Git pull to get recent changes
git pull

# Run the jobs file 
bsub jobs.sh

# Chain next job
at now + 1 day -f runme.sh -m
