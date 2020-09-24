import os
from os import listdir
from os.path import isfile, join
import subprocess
from shutil import copyfile
from time import time
from time import sleep
if __name__ == "__main__":
    t1 = time()
    srcPath = "/home/ronald/dataTemp"
    destPath = "/home/ronald/data"

    if not os.path.exists(destPath):
        os.makedirs(destPath)

    files = [f for f in listdir(srcPath) if isfile(join(srcPath, f))]
    sleep(7)
    for i in files:
        copyfile(join(srcPath,i), join(destPath,i))

    print('Copying all files done in {} seconds'.format(time()-t1))
