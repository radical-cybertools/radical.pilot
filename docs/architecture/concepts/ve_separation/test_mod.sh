#!/bin/sh

. /usr/share/modules/init/sh
module use $PWD/modules
module load my_python

./test_mod.py

