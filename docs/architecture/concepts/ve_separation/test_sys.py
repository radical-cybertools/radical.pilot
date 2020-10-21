#!/usr/bin/env python3

import os

# print(os.__file__)
print('sys  : ', os.getpid(), bool('/usr/lib/' in os.__file__))

