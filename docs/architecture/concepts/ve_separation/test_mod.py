#!/usr/bin/env python3

import os

# print(os.__file__)
print('mod  : ', os.getpid(), bool('/my_python_dir/' in os.__file__))

