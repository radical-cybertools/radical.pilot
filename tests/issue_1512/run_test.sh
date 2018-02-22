#!/bin/bash

for jj in {512,1024,2048}; do
    python randomFiles.py $jj
    echo '----------Number of files: '$jj'----------'
    echo '---------- Half Meg ----------'
    echo 'Time'>run_half_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py halfMeg wrangler.tacc.utexas.edu /data/02876/srini216/halfMeg >>run_half_$jj.csv 2>>log.log
    done
    echo '---------- One Meg ----------'
    echo 'Time'>run_one_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py meg wrangler.tacc.utexas.edu /data/02876/srini216/meg >>run_one_$jj.csv 2>>log.log
    done
    echo '---------- Two Meg ----------'
    echo 'Time'>run_two_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py twoMeg wrangler.tacc.utexas.edu /data/02876/srini216/twoMeg >>run_two_$jj.csv 2>>log.log
    done



    python randomFiles.py $jj
    echo '----------Number of files with Tar: '$jj'----------'
    echo '---------- Half Meg ----------'
    echo 'Time'>runTar_half_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py halfMeg wrangler.tacc.utexas.edu /data/02876/srini216/halfMeg --tar >>runTar_half_$jj.csv 2>>log.log
            rm -rf tartest.tar
    done
    echo '---------- One Meg ----------'
    echo 'Time'>runTar_one_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py meg wrangler.tacc.utexas.edu /data/02876/srini216/meg --tar >>runTar_one_$jj.csv 2>>log.log
            rm -rf tartest.tar
    done
    echo '---------- Two Meg ----------'
    echo 'Time'>runTar_two_$jj.csv
    for ii in {1..10}; do
            python transfer_files.py twoMeg wrangler.tacc.utexas.edu /data/02876/srini216/twoMeg --tar >>runTar_two_$jj.csv 2>>log.log
            rm -rf tartest.tar
    done
done