#!/bin/bash

for jj in {1,2,4,8,16,32,64,128,256,512,1024,2048}; do
    python randomFiles.py $jj
    echo '----------Number of files: '$jj'----------'
    echo '---------- Half Meg ----------'
    echo 'Time'>run_half_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py halfMeg wrangler.tacc.utexas.edu <path> >>run_half_$jj.csv 2>>log.log
    done
    echo '---------- One Meg ----------'
    echo 'Time'>run_one_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py meg wrangler.tacc.utexas.edu <path> >>run_one_$jj.csv 2>>log.log
    done
    echo '---------- Two Meg ----------'
    echo 'Time'>run_two_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py twoMeg wrangler.tacc.utexas.edu <path> >>run_two_$jj.csv 2>>log.log
    done
done

for jj in {1,2,4,8,16,32,64,128,256,512,1024,2048}; do
    python randomFiles.py $jj
    echo '----------Number of files with Tar: '$jj'----------'
    echo '---------- Half Meg ----------'
    echo 'Time'>runTar_half_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py halfMeg wrangler.tacc.utexas.edu <path> --tar >>runTar_half_$jj.csv 2>>log.log
            rm -rf tartest.tar
    done
    echo '---------- One Meg ----------'
    echo 'Time'>runTar_one_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py meg wrangler.tacc.utexas.edu <path> --tar >>runTar_one_$jj.csv 2>>log.log
            rm -rf tartest.tar
    done
    echo '---------- Two Meg ----------'
    echo 'Time'>runTar_two_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py twoMeg wrangler.tacc.utexas.edu <path> --tar >>runTar_two_$jj.csv 2>>log.log
            rm -rf tartest.tar
    done
done