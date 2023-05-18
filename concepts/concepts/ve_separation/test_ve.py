#!/usr/bin/env python3

import os

# print(os.__file__)
print('ve   : ', os.getpid(), bool('/ve_test/lib/' in os.__file__))

