import os
import tarfile
from shutil import copyfile as cp
import sys


tar = tarfile.open("tartest.tar","w")
for filename in ["foldername"]:
    tar.add(filename)
tar.close


cp(sys.argv[1], sys.argv[2])


untar = tarfile.open("tartest.tar")
untar.extractall()
untar.close


