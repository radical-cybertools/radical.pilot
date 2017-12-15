import os
import tarfile
from shutil import copyfile as cp
import sys
import glob
from time import time

if __name__ == '__main__':

    
    folderFiles = glob.glob(sys.argv[1]+'/*')
    start = time()
    tar = tarfile.open("tartest.tar","w")
    for filename in folderFiles:
        tar.add(filename)
    tar.close()
    creation = time()-start
    cp(sys.argv[2], sys.argv[3])
    copyTime = time()-creation - start

    untar = tarfile.open("tartest.tar")
    untar.extractall()
    untar.close()

    extractTime = time() - copyfile - creation - start

    print(creation,copyTime,extractTime)