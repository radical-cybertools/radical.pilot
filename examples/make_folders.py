#!/usr/bin/env python3

import os
import sys


# ------------------------------------------------------------------------------
#

for i in range(int(sys.argv[1])):

    path = '/tmp/stage_in_folder_%d' % i

    if not os.path.exists(path):
        os.makedirs(path)

    with open('%s/input_file.dat' % path, 'w') as fin:
        fin.write('hello world!\n')


# ------------------------------------------------------------------------------

