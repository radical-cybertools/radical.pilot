#!/usr/bin/env python3

import os

# print(os.__file__)
print('conda: ', os.getpid(), bool('envs/conda_test/lib/' in os.__file__))

