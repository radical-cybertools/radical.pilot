__author__    = "George Chantzialexiou"
__copyright__ = "Copyright 2014, The RADICAL Group"
__license__   = "MIT"

import sys
import random

#------------------------------------------------------------------------------

if __name__ == "__main__":

    args = sys.argv[1:]
    if len(args) < 1:
        print "Usage: python %s needs the  number of the dataset. Please run again" % __file__
        print "python create_dataset.py n"
        sys.exit(-1)
    
    n = int(sys.argv[1])  # k is the number of clusters i want to create
    # read the dataset from dataset.data f
    
    #open the file where we write the results
    data_file = open('dataset2.data', 'w')
    
    
    for i in range(0,n-1):
        a = random.uniform(0, 10000)
        data_file.write(str(a))
        data_file.write(',')
    
    # writing the last element separately, because we don't need to add a coma
    a = random.uniform(0, 10000)
    data_file.write(str(a))
    
    data_file.close()

    sys.exit(0)


#------------------------------------------------------------------------------

