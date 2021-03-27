#!/bin/sh

. $HOME/.miniconda3/etc/profile.d/conda.sh
conda activate conda_test

./test_conda.py

