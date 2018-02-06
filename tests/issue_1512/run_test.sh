#!/bin/bash

for jj in {1,2,4,8,16,32,64,128,256,512,1024,2048}; do
		echo '----------Number of files: ',$jj,'----------'
		echo '---------- Half Meg ----------'
        echo 'Time'>run_half_ $jj.csv
        for ii in {1..10}; do
                python transfer_files.py halfMeg wrangler.tacc.utexas.edu <path> >>run_$jj.csv
        done
		echo '---------- One Meg ----------'
        echo 'Time'>run_half_ $jj.csv
        for ii in {1..10}; do
                python transfer_files.py meg wrangler.tacc.utexas.edu <path> >>run_$jj.csv
        done
		echo '---------- Two Meg ----------'
        echo 'Time'>run_half_ $jj.csv
        for ii in {1..10}; do
                python transfer_files.py twoMeg wrangler.tacc.utexas.edu <path> >>run_$jj.csv
        done
done