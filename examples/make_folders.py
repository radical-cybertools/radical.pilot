import os
import sys


nfolders = int(sys.argv[1])

for i in range(nfolders):

    path = '/tmp/stage_in_folder_%d' % i

    if not os.path.exists(path):
        os.makedirs(path)

    with open('/tmp/stage_in_folder_%d/input_file.dat' % i, 'w') as fin:
        fin.write('hello world!')

