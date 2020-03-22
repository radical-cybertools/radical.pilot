#!/usr/bin/env python3

import os

# print(os.__file__)
print('exe  : ', os.getpid(), bool('/ve/lib/' in os.__file__))

os.system('/bin/sh test_sys.sh')
os.system('/bin/sh test_ve.sh')
os.system('/bin/sh test_conda.sh')

