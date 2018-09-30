import os
import sys



number_of_folders = int(sys.argv[1])

for i in xrange(number_of_folders):
    directory = '/tmp/stage_in_folder_%d' % i 
    if not os.path.exists(directory):
        os.makedirs(directory)

    filename = '/tmp/stage_in_folder_%d/input_file.dat'%i
    afile = open(filename,'w')
    afile.write('hello world!')
    afile.close()
